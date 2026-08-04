use clap::{ArgGroup, Parser};
// use fxhash::FxHashSet;
use multi_summaries::instrument;
use multi_summaries::instrumentation::print_format_last;
use multi_summaries::partition_proto::{PbPart, PbPartition};
use prost::Message;
// use multi_summaries::instrumentation::serialize_stats;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::{self, File};
// use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Result, Write, sink};
use std::path::{Path, PathBuf};
use std::println;
use time::{
    OffsetDateTime, format_description::StaticFormatDescription, macros::format_description,
};

// use itertools::Itertools;
use multi_summaries::graph::{FlatGraph, Graph};
// use multi_summaries::graph::EdgeType;

use multi_summaries::bisimulator::{
    BlockAssignment, FullBisimulationState, GlobalBlockIndexAndLevel, get_0_bisimulation,
    get_i_bisimulation, get_typed_0_bisimulation,
};

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

static FMT: StaticFormatDescription = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
);

#[derive(Serialize, Deserialize)]
struct BisimulationStatistics {
    singletons_condensed: Vec<usize>,
    singletons_uncondensed: Vec<usize>,
    blocks_quotient: Vec<usize>,
    blocks_condensed: Vec<usize>,
    blocks_uncondensed: Vec<usize>,
    refines_edges_condensed: Vec<usize>,
    data_edges_quotient: Vec<usize>,
    data_edges_condensed: Vec<usize>,
    data_edges_uncondensed: Vec<usize>,
}

impl BisimulationStatistics {
    fn new() -> Self {
        BisimulationStatistics {
            singletons_condensed: Vec::new(),
            singletons_uncondensed: Vec::new(),
            blocks_quotient: Vec::new(),
            blocks_condensed: Vec::new(),
            blocks_uncondensed: Vec::new(),
            refines_edges_condensed: Vec::new(),
            data_edges_quotient: Vec::new(),
            data_edges_condensed: Vec::new(),
            data_edges_uncondensed: Vec::new(),
        }
    }

    fn add_level(&mut self, bisimulation_state: &FullBisimulationState<impl Write, impl Write>) {
        let now = OffsetDateTime::now_local()
            .expect("time could not get the local time")
            .format(&FMT)
            .unwrap();

        let last_singletons_uncondensed = self.singletons_uncondensed.last().copied().unwrap_or(0);
        let last_blocks_condensed = self
            .blocks_condensed
            .last()
            .copied()
            .unwrap_or(bisimulation_state.current_outcome.total_blocks());
        let last_blocks_uncondensed = self.blocks_uncondensed.last().copied().unwrap_or(0);
        let refines_edges_condensed = self.refines_edges_condensed.last().copied().unwrap_or(0);

        self.singletons_uncondensed
            .push(last_singletons_uncondensed + bisimulation_state.current_outcome.singletons());
        self.blocks_condensed
            .push(last_blocks_condensed + bisimulation_state.shared_state.refines_edge_count());
        self.blocks_uncondensed
            .push(last_blocks_uncondensed + bisimulation_state.current_outcome.total_blocks());
        self.refines_edges_condensed
            .push(refines_edges_condensed + bisimulation_state.shared_state.refines_edge_count());

        self.singletons_condensed
            .push(bisimulation_state.current_outcome.singletons());
        self.blocks_quotient
            .push(bisimulation_state.current_outcome.total_blocks());

        println!(
            "{} - After computing {:>4}-bisimulation --> Dirty blocks: {:<10}, singletons: {:<10}, blocks {:<10}, blocks (condensed) {:<10}, singletons (uncondensed) {:<10} blocks (uncondensed) {:<10}, refines edges ((un)condensed) {:<10}",
            now,
            bisimulation_state.shared_state.i - 1,
            bisimulation_state.current_outcome.dirty_blocks.len(),
            self.singletons_condensed.last().unwrap(),
            self.blocks_quotient.last().unwrap(),
            self.blocks_condensed.last().unwrap(),
            self.singletons_uncondensed.last().unwrap(),
            self.blocks_uncondensed.last().unwrap(),
            self.refines_edges_condensed.last().unwrap(),
        );
    }

    // fn add_aggregate_data_edges(&mut self, aggregate_counter: DataEdgeCounter) {
    //     assert!(self.data_edges_condensed.is_empty() && self.data_edges_uncondensed.is_empty());
    //     self.data_edges_condensed = aggregate_counter.condensed_counts;
    //     self.data_edges_uncondensed = aggregate_counter.uncondensed_counts
    // }
}

// #[derive(Clone)]
// struct DataEdgeAndInterval {
//     pub data_edge: (GlobalBlockIndex, EdgeType, GlobalBlockIndex),
//     pub interval: (LevelIndex, LevelIndex),
// }

// impl Hash for DataEdgeAndInterval {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         state.write_usize(self.data_edge.0);
//         state.write_u32(self.data_edge.1);
//         state.write_usize(self.data_edge.2);
//     }
// }

// // Data edges are uniquely identified by their triples, so we can ignore the intervals for the purposes of equality and ordering
// impl PartialEq for DataEdgeAndInterval {
//     fn eq(&self, other: &Self) -> bool {
//         self.data_edge == other.data_edge
//     }
// }

// impl Eq for DataEdgeAndInterval {}

// impl PartialOrd for DataEdgeAndInterval {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Ord for DataEdgeAndInterval {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.data_edge.cmp(&other.data_edge)
//     }
// }

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
    // let statistics_path = output_path_buf.join("statistics.json");
    // let statistics_file = File::create(statistics_path)?;

    // 3. Iterative Refinement
    for _ in 1u64..=max_k {
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

    // 4. Emit the data edges for the remaining (non-singleton) blocks
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!(
        "{} - Emitting data edges for final (non-singleton) blocks...",
        now
    );
    let (mut final_state, final_outcome) = bisimulation_state.into_parts();
    let singleton_mapping = std::mem::take(&mut final_state.singleton_mapping);
    let block_mapping = std::mem::take(&mut final_state.previous_block_mapping);
    // instrument!("Emit Final Blocks", {
    //     // This hashset it reused many times in the next for loop
    //     let mut outer_data_edges: FxHashSet<DataEdgeAndInterval> = FxHashSet::default();

    //     for (block_idx, block) in std::mem::take(&mut final_outcome.blocks)
    //         .into_iter()
    //         .enumerate()
    //         .filter_map(|(block_idx, maybe_block)| maybe_block.map(|block| (block_idx, block)))
    //     {
    //         let GlobalBlockIndexAndLevel {
    //             global_id: global_subject,
    //             level: subject_level,
    //         } = block_mapping.get(&block_idx).unwrap();

    //         outer_data_edges.clear();
    //         for node_idx in block.nodes.iter() {
    //             for edge in graph.get_node(*node_idx).edges.iter() {
    //                 let edge_type = edge.label;
    //                 let GlobalBlockIndexAndLevel {
    //                     global_id: global_target,
    //                     level: target_level,
    //                 } = match &final_outcome.node_to_block.mapping[edge.target] {
    //                     BlockAssignment::Block(block_id) => block_mapping.get(block_id).unwrap(),
    //                     BlockAssignment::Singleton(singleton_id) => {
    //                         singleton_mapping.get(singleton_id).unwrap()
    //                     }
    //                 };
    //                 let start_time = std::cmp::max(*subject_level, target_level + 1);
    //                 let end_time = 0; // NB: we are using 0 to encode for infinity
    //                 outer_data_edges.insert(DataEdgeAndInterval {
    //                     data_edge: (*global_subject, edge_type, *global_target),
    //                     interval: (start_time, end_time),
    //                 });
    //             }
    //         }

    //         for DataEdgeAndInterval {
    //             data_edge,
    //             interval,
    //         } in outer_data_edges.iter()
    //         {
    //             final_state.data_edge_callback(*data_edge, *interval)?;
    //         }
    //     }
    // });
    // print_format_last("\n", "\n");

    // // 5. Emit the data edges for the remaining singleton blocks
    // let now = OffsetDateTime::now_local()
    //     .expect("time could not get the local time")
    //     .format(&FMT)
    //     .unwrap();
    // println!("{} - Emitting data edges for final singletons...", now);
    // instrument!(
    //     "Emit Final Singletons",
    //     for (
    //         node_idx,
    //         GlobalBlockIndexAndLevel {
    //             global_id: global_subject,
    //             level: subject_level,
    //         },
    //     ) in singleton_mapping.iter()
    //     {
    //         let mut inner_data_edges = Vec::new();
    //         for edge in graph.get_node(*node_idx).edges.iter() {
    //             let edge_type = edge.label;
    //             let GlobalBlockIndexAndLevel {
    //                 global_id: global_target,
    //                 level: target_level,
    //             } = match &final_outcome.node_to_block.mapping[edge.target] {
    //                 BlockAssignment::Block(block_id) => {
    //                     block_mapping.get(block_id).copied().unwrap()
    //                 }
    //                 BlockAssignment::Singleton(singleton_id) => {
    //                     singleton_mapping.get(singleton_id).copied().unwrap()
    //                 }
    //             };
    //             let start_time = std::cmp::max(*subject_level, target_level + 1);
    //             let end_time = 0; // final_state.i-1;
    //             inner_data_edges.push(DataEdgeAndInterval {
    //                 data_edge: (*global_subject, edge_type, global_target),
    //                 interval: (start_time, end_time),
    //             });
    //         }
    //         inner_data_edges.sort();
    //         inner_data_edges.dedup();
    //         for DataEdgeAndInterval {
    //             data_edge,
    //             interval,
    //         } in inner_data_edges.into_iter()
    //         {
    //             final_state.data_edge_callback(data_edge, interval)?;
    //         }
    //     }
    // );
    // print_format_last("", "\n");

    // // Explicit flush is good practice, though it happens automatically on drop
    // final_state.data_edge_writer.flush()?;
    // final_state.refines_writer.flush()?;

    // // Get the data edge statistics
    // let data_edge_counts = final_state.data_edge_counter();
    // bisimulation_statistics.add_aggregate_data_edges(data_edge_counts);

    // // Serialize the bisimulation statistics
    // serde_json::to_writer_pretty(statistics_file, &bisimulation_statistics)?;

    // // Serialize the instrumentation statistics
    // serialize_stats(output_path_buf.join("instrumentation.json"))?;

    // Emit the final outcome
    let final_node_to_block_path = output_path_buf.join("final_node_to_block");
    let final_node_to_block_file = File::create(final_node_to_block_path)?;
    let mut final_node_to_block_writer = BufWriter::new(final_node_to_block_file);

    // let mut largest_block_id = 0;
    let mut node_to_block_ids = Vec::with_capacity(final_outcome.node_to_block.mapping.len());
    // let mut num_nodes = 0;
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
            // largest_block_id = max(largest_block_id, *global_id);
            node_to_block_ids.push(*global_id);
            // num_nodes += 1;
        });
    let slice_partition = FixedDenseByteSlicePartition::from_node_to_ids(node_to_block_ids);
    // let mut test_hash_set = HashSet::new();
    // let mut num_blocks = 0;
    // slice_partition.into_iter().for_each(|slice| {
    //     for node in slice {
    //         test_hash_set.insert(*node);
    //     }
    //     num_blocks += 1;
    // });
    // println!(
    //     "Min: {}\nMax: {}\nLen: {}\nNum Nodes: {}\nLargest Block: {}\nNum Blocks: {}\n",
    //     test_hash_set.iter().min().unwrap(),
    //     test_hash_set.iter().max().unwrap(),
    //     test_hash_set.len(),
    //     num_nodes,
    //     largest_block_id,
    //     num_blocks,
    // );

    let wire_parts: Vec<PbPart> = slice_partition.into_iter().map(|block| PbPart {
        nodes: block.into(),
        hash: Vec::new(),
    }).collect();

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

    // for (node, local_block) in final_outcome.node_to_block.mapping.into_iter().enumerate() {
    //     let global_block = match local_block {
    //         BlockAssignment::Block(local_block_id) => block_mapping.get(&local_block_id).unwrap(),
    //         BlockAssignment::Singleton(node_id) => singleton_mapping.get(&node_id).unwrap(),
    //     };
    //     final_node_to_block_writer.write_all(&node.to_be_bytes())?;
    //     final_node_to_block_writer.write_all(&global_block.global_id.to_be_bytes())?;
    // }

    // for (node, local_block) in final_outcome.node_to_block.mapping.iter().enumerate() {
    //     let global_block = match local_block {
    //         BlockAssignment::Block(local_block_id) => block_mapping.get(local_block_id).unwrap(),
    //         BlockAssignment::Singleton(node_id) => singleton_mapping.get(node_id).unwrap(),
    //     };
    //     final_node_to_block_writer.write_all(&node.to_be_bytes())?;
    //     final_node_to_block_writer.write_all(&global_block.global_id.to_be_bytes())?;
    // }

    // final_node_to_block_writer.flush()?;

    // Ok(())
}
