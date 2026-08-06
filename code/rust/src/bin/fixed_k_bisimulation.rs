use clap::{ArgGroup, Parser};
use multi_summaries::bisimulation_statistics::{BisimulationStatistics, FMT};
use multi_summaries::bisimulator::{
    BlockAssignment, FullBisimulationState, GlobalBlockIndexAndLevel, get_0_bisimulation,
    get_i_bisimulation, get_typed_0_bisimulation,
};
use multi_summaries::graph::{FlatGraph, Graph};
use multi_summaries::instrument;
use multi_summaries::instrumentation::{print_format_last, serialize_stats};
use multi_summaries::partition_proto::{PbPart, PbPartition};
use prost::Message;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Result, Write, sink};
use std::path::{Path, PathBuf};
use std::println;
use time::OffsetDateTime;

#[derive(Parser, Debug)]
#[command(group(
    ArgGroup::new("source")
        .args(&["type_relation_id", "rel_to_id_file"])
        .multiple(false)   // mutually exclusive
))]
struct Cli {
    /// The input graph in binary format
    input: PathBuf,

    /// The output directory for the generated files
    output: PathBuf,

    /// The id for the relation used for splitting at iteration 0
    #[arg(long)]
    type_relation_id: Option<u32>,

    /// A file containing the rdf:type relation along with its id used for splitting at iteration 0
    #[arg(long)]
    rel_to_id_file: Option<PathBuf>,

    /// The minimal block size needed to be eligible to split
    #[arg(long)]
    min_support: Option<usize>,

    /// The maximum bisimulation depth to search
    #[arg(long)]
    max_k: u64,

    /// Preallocation size
    #[arg(long)]
    preallocation_size: Option<usize>,
}

// TODO this function could also be extended to work on "rel2ID.meta.json" files
fn parse_rel_to_id(rel_to_id_path: impl AsRef<Path>) -> Result<Option<u32>> {
    const RDF_TYPE_RELATION_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let file = File::open(rel_to_id_path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let mut parts = line.split_whitespace();

        if let (Some(k), Some(v)) = (parts.next(), parts.next())
            && k == RDF_TYPE_RELATION_STRING
        {
            if let Ok(num) = v.parse::<u32>() {
                return Ok(Some(num));
            }
            return Err(Error::new(ErrorKind::InvalidData, "Invalid number"));
        }
    }

    Ok(None)
}

struct FixedDenseByteSlicePartition {
    data: Vec<i64>,
    offsets: Vec<usize>,
}

impl FixedDenseByteSlicePartition {
    fn from_node_to_ids(node_to_block_ids: impl Into<Vec<usize>>) -> Self {
        let node_to_block_ids = node_to_block_ids.into();
        let num_blocks = match node_to_block_ids.iter().max() {
            Some(largest_block_id) => *largest_block_id + 1,
            None => {
                return FixedDenseByteSlicePartition {
                    data: Vec::new(),
                    offsets: Vec::new(),
                };
            }
        };

        // 1. Get the offsets for each partition
        // Each offset will point to the start of a partition
        // Note that missing block IDs will simply result in streaks of equal offsets
        let offsets = {
            let mut offsets = vec![0; num_blocks];
            node_to_block_ids.iter().for_each(|block_id| {
                offsets[*block_id] += 1;
            });
            let mut acc = 0;
            offsets.iter_mut().for_each(|offset| {
                let offset_copy = *offset;
                *offset = acc;
                acc += offset_copy;
            });
            offsets
        };

        // 2. Initialize the lengths
        // Note that the lengths are only needed until `data` has been fully populizes
        let mut lengths = vec![0; num_blocks];

        // 3. Populate the vector, without initialization or bounds checks.
        // By this point, the difference between each offset should exactly match the size of each block (num_nodes - offset for the last partition).
        // This invariant guarantees that the full vector gets initialized, without going out of bounds.
        let num_nodes = node_to_block_ids.len();
        let mut data = Vec::<i64>::with_capacity(num_nodes);
        let spare = data.spare_capacity_mut();

        for (node_id, block_id) in node_to_block_ids.into_iter().enumerate() {
            let offset = unsafe { offsets.get_unchecked(block_id) };
            let length = unsafe { lengths.get_unchecked_mut(block_id) };

            debug_assert!(
                *offset + *length < *offsets.get(block_id + 1).unwrap_or(&num_nodes),
                "partition overflow"
            );

            spare[*offset + *length].write(node_id as i64);
            *length += 1;
        }

        unsafe {
            data.set_len(num_nodes);
        }

        FixedDenseByteSlicePartition { data, offsets }
    }
}

struct PartitionIter<'a> {
    data: &'a [i64],
    offsets: &'a [usize],
    next_block: usize,
}

impl<'a> Iterator for PartitionIter<'a> {
    type Item = &'a [i64];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_block >= self.offsets.len() {
            return None;
        }

        let start = self.offsets[self.next_block];

        let end = self
            .offsets
            .get(self.next_block + 1)
            .copied()
            .unwrap_or(self.data.len());

        self.next_block += 1;

        Some(&self.data[start..end])
    }
}

impl<'a> IntoIterator for &'a FixedDenseByteSlicePartition {
    type Item = &'a [i64];
    type IntoIter = PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter {
            data: &self.data,
            offsets: &self.offsets,
            next_block: 0,
        }
    }
}

fn main() -> Result<()> {
    // Parse arguments
    let args = Cli::parse();
    let input_file = &args.input;
    let output_dir = &args.output;
    if args.output.is_dir() {
        return Err(Error::new(
            ErrorKind::AlreadyExists,
            format!("Output directory '{}' already exist", args.output.display()),
        ));
    }
    fs::create_dir_all(output_dir)?;
    let type_id = match (&args.type_relation_id, &args.rel_to_id_file) {
        (Some(edge_type), None) => Some(*edge_type),
        (None, Some(rel_to_id_path)) => parse_rel_to_id(rel_to_id_path)?.or_else(|| {
            eprintln!("Warning rel_to_id_file was provided, but no 'rdf:type' relation was found");
            None
        }),
        (None, None) => None,
        _ => unreachable!("Clap's ArgGroup should prevent case"),
    };
    let min_support = args.min_support.unwrap_or(0);
    let max_k = args.max_k;
    let preallocation_size = args.preallocation_size.unwrap_or(1_000_000_000);

    // Create graph
    let mut g = Graph::new(preallocation_size);
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Loading Graph...", now);
    instrument!(
        "Loading Graph",
        g.read_graph_parallel_memmmap(input_file, false)?
    );
    print_format_last("\n", "\n");

    // Run bisimulation
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Converting Graph...", now);
    let flat_graph = instrument!("Converting Graph", FlatGraph::new(g));
    print_format_last("", "\n");
    compute_bisimulation(&flat_graph, output_dir, min_support, type_id, max_k)?;

    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Bisimulation Complete!", now);
    Ok(())
}

pub fn compute_bisimulation(
    graph: &FlatGraph,
    output_dir: impl AsRef<Path>,
    min_support: usize,
    type_id: Option<u32>,
    max_k: u64,
) -> Result<()> {
    // 1. Prepare the Graph: Build the reverse index needed for dirty propagation
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Building predecessor index...", now);
    let predecessors = graph.build_predecessors();

    // 2. Initial Partition: Level 0 (All nodes in one block)
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Computing 0-bisimulation...", now);

    let zero_outcome = instrument!(
        "0-Bisimulation",
        match type_id {
            Some(edge_type) => get_typed_0_bisimulation(graph, edge_type),
            None => get_0_bisimulation(graph),
        }
    );

    let output_path_buf = output_dir.as_ref().to_path_buf();
    let mut bisimulation_state = {
        // We only care about the final outcome, so dump the refines and data edges into a sink
        let refines_writer = sink();
        let data_edge_writer = sink();
        FullBisimulationState::new(zero_outcome, refines_writer, data_edge_writer)?
    };
    // let mut bisimulation_state = FullBisimulationState::new(zero_outcome, output_path_buf.clone())?;

    let mut bisimulation_statistics = BisimulationStatistics::new();
    let statistics_path = output_path_buf.join("statistics.json");
    let statistics_file = File::create(statistics_path)?;

    // 3. Iterative Refinement
    let mut stabilized = false;
    for _ in 0..max_k {
        bisimulation_statistics.add_level(&bisimulation_state);
        print_format_last("", "\n");

        // If no blocks are dirty, the partition is stable, but if semi_dirty_blocks is not empty, we still need to emit some of there associated data edges
        if bisimulation_state.current_outcome.dirty_blocks.is_empty() {
            let fixed_point = bisimulation_state.shared_state.i - 1;
            if !bisimulation_state
                .current_outcome
                .semi_dirty_blocks
                .is_empty()
            {
                let now = OffsetDateTime::now_local()
                    .expect("time could not get the local time")
                    .format(&FMT)
                    .unwrap();
                println!(
                    "{} - Running extra iteration to emit data edges that end at the fixed point",
                    now
                );
                instrument!(
                    "Extra Iteration",
                    bisimulation_state =
                        get_i_bisimulation(graph, &predecessors, bisimulation_state, min_support)?
                );
                print_format_last("", "\n")
            }
            let now = OffsetDateTime::now_local()
                .expect("time could not get the local time")
                .format(&FMT)
                .unwrap();
            stabilized = true;
            println!("{} - Bisimulation stabilized at k = {}", now, fixed_point);
            break;
        }

        // Perform the refinement step
        instrument!(
            format!("{}-Bisimulation", bisimulation_state.shared_state.i.clone()),
            bisimulation_state =
                get_i_bisimulation(graph, &predecessors, bisimulation_state, min_support)?
        );

        // Update state
        bisimulation_state.shared_state.update_level()?;
    }

    if !stabilized {
        bisimulation_statistics.add_level(&bisimulation_state);
    }

    // 4. Emit the data edges for the remaining (non-singleton) blocks
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    let current_level = bisimulation_state.shared_state.i as usize - 1;
    let num_blocks = *bisimulation_statistics
        .get_borrowed_view()
        .blocks_quotient
        .get(current_level)
        .unwrap();
    println!(
        "{} - Emitting the partition for K={}, with {} blocks",
        now, current_level, num_blocks,
    );
    let (mut final_state, final_outcome) = bisimulation_state.into_parts();
    let singleton_mapping = std::mem::take(&mut final_state.singleton_mapping);
    let block_mapping = std::mem::take(&mut final_state.previous_block_mapping);

    // Get the data edge statistics
    let data_edge_counts = final_state.data_edge_counter();
    bisimulation_statistics.add_aggregate_data_edges(data_edge_counts);

    // Serialize the bisimulation statistics
    serde_json::to_writer_pretty(statistics_file, &bisimulation_statistics)?;

    // Serialize the instrumentation statistics
    serialize_stats(output_path_buf.join("instrumentation.json"))?;

    // Emit the final outcome
    let final_node_to_block_path = output_path_buf.join("proto_partition");
    let final_node_to_block_file = File::create(final_node_to_block_path)?;
    let mut final_node_to_block_writer = BufWriter::new(final_node_to_block_file);

    let mut node_to_block_ids = Vec::with_capacity(final_outcome.node_to_block.mapping.len());
    final_outcome
        .node_to_block
        .mapping
        .into_iter()
        .for_each(|block_assignment| {
            let GlobalBlockIndexAndLevel {
                global_id,
                level: _,
            } = match block_assignment {
                BlockAssignment::Block(local_block_id) => {
                    block_mapping.get(&local_block_id).unwrap()
                }
                BlockAssignment::Singleton(node_id) => singleton_mapping.get(&node_id).unwrap(),
            };
            node_to_block_ids.push(*global_id);
        });
    let slice_partition = FixedDenseByteSlicePartition::from_node_to_ids(node_to_block_ids);

    let wire_parts: Vec<PbPart> = slice_partition
        .into_iter()
        .filter_map(|block| {
            if block.is_empty() {
                return None;
            }
            Some(PbPart {
                nodes: block.into(),
                hash: Vec::new(),
            })
        })
        .collect();

    let wire_struct = PbPartition { parts: wire_parts };

    // Encode
    let mut buf = Vec::new();
    wire_struct
        .encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Write
    final_node_to_block_writer.write_all(&buf)?;
    final_node_to_block_writer.flush()?;
    Ok(())
}
