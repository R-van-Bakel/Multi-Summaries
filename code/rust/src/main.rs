use clap::{ArgGroup, Parser};
use fxhash::FxHashSet;
use multi_summaries::bisimulation_statistics::{BisimulationStatistics, FMT};
use multi_summaries::bisimulator::{
    BlockAssignment, FullBisimulationState, GlobalBlockIndex, GlobalBlockIndexAndLevel, LevelIndex,
    get_0_bisimulation, get_i_bisimulation, get_typed_0_bisimulation,
};
use multi_summaries::graph::{EdgeType, FlatGraph, Graph};
use multi_summaries::instrument;
use multi_summaries::instrumentation::{print_format_last, serialize_stats};
use multi_summaries::writers::{RotateBufWriter, RotateWrite};
use std::fmt::Debug;
use std::fs::{self, File};
use std::hash::Hash;
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Result, Write};
use std::path::{Path, PathBuf};
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

    /// Perform backwards bisimulation instead of forwards.
    #[arg(long)]
    backwards: bool,

    /// The minimal block size needed to be eligible to split
    #[arg(long)]
    min_support: Option<usize>,

    /// The maximum bisimulation depth to search
    #[arg(long)]
    max_k: Option<u64>,

    /// Preallocation size
    #[arg(long)]
    preallocation_size: Option<usize>,
}

#[derive(Clone)]
struct DataEdgeAndInterval {
    pub data_edge: (GlobalBlockIndex, EdgeType, GlobalBlockIndex),
    pub interval: (LevelIndex, LevelIndex),
}

impl Hash for DataEdgeAndInterval {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_usize(self.data_edge.0);
        state.write_u32(self.data_edge.1);
        state.write_usize(self.data_edge.2);
    }
}

// Data edges are uniquely identified by their triples, so we can ignore the intervals for the purposes of equality and ordering
impl PartialEq for DataEdgeAndInterval {
    fn eq(&self, other: &Self) -> bool {
        self.data_edge == other.data_edge
    }
}

impl Eq for DataEdgeAndInterval {}

impl PartialOrd for DataEdgeAndInterval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DataEdgeAndInterval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data_edge.cmp(&other.data_edge)
    }
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
        g.read_graph_parallel_memmmap(input_file, args.backwards)?
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
    max_k: Option<u64>,
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
        let refines_writer = RotateBufWriter::new(output_path_buf.join("refines"), 1);
        let data_edge_writer = BufWriter::new(File::create(output_path_buf.join("data_edges"))?);
        FullBisimulationState::new(zero_outcome, refines_writer, data_edge_writer)?
    };

    let mut bisimulation_statistics = BisimulationStatistics::new();
    let statistics_path = output_path_buf.join("statistics.json");
    let statistics_file = File::create(statistics_path)?;

    // 3. Iterative Refinement
    loop {
        bisimulation_statistics.add_level(&bisimulation_state);
        print_format_last("", "\n");

        // Break if we've reached a user-defined depth limit
        if let Some(limit) = max_k
            && bisimulation_state.shared_state.i >= limit
        {
            break;
        }

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

        // Update state and rotate the refines_writer to the next file
        bisimulation_state.shared_state.update_level()?;
        bisimulation_state
            .shared_state
            .refines_writer_mut()
            .rotate();
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
    let (mut final_state, mut final_outcome) = bisimulation_state.into_parts();
    let singleton_mapping = std::mem::take(&mut final_state.singleton_mapping);
    let block_mapping = std::mem::take(&mut final_state.previous_block_mapping);
    instrument!("Emit Final Blocks", {
        // This hashset it reused many times in the next for loop
        let mut outer_data_edges: FxHashSet<DataEdgeAndInterval> = FxHashSet::default();

        for (block_idx, block) in std::mem::take(&mut final_outcome.blocks)
            .into_iter()
            .enumerate()
            .filter_map(|(block_idx, maybe_block)| maybe_block.map(|block| (block_idx, block)))
        {
            let GlobalBlockIndexAndLevel {
                global_id: global_subject,
                level: subject_level,
            } = block_mapping.get(&block_idx).unwrap();

            outer_data_edges.clear();
            for node_idx in block.nodes.iter() {
                for edge in graph.get_node(*node_idx).edges.iter() {
                    let edge_type = edge.label;
                    let GlobalBlockIndexAndLevel {
                        global_id: global_target,
                        level: target_level,
                    } = match &final_outcome.node_to_block.mapping[edge.target] {
                        BlockAssignment::Block(block_id) => block_mapping.get(block_id).unwrap(),
                        BlockAssignment::Singleton(singleton_id) => {
                            singleton_mapping.get(singleton_id).unwrap()
                        }
                    };
                    let start_time = std::cmp::max(*subject_level, target_level + 1);
                    let end_time = 0; // NB: we are using 0 to encode for infinity
                    outer_data_edges.insert(DataEdgeAndInterval {
                        data_edge: (*global_subject, edge_type, *global_target),
                        interval: (start_time, end_time),
                    });
                }
            }

            for DataEdgeAndInterval {
                data_edge,
                interval,
            } in outer_data_edges.iter()
            {
                final_state.data_edge_callback(*data_edge, *interval)?;
            }
        }
    });
    print_format_last("\n", "\n");

    // 5. Emit the data edges for the remaining singleton blocks
    let now = OffsetDateTime::now_local()
        .expect("time could not get the local time")
        .format(&FMT)
        .unwrap();
    println!("{} - Emitting data edges for final singletons...", now);
    instrument!(
        "Emit Final Singletons",
        for (
            node_idx,
            GlobalBlockIndexAndLevel {
                global_id: global_subject,
                level: subject_level,
            },
        ) in singleton_mapping.iter()
        {
            let mut inner_data_edges = Vec::new();
            for edge in graph.get_node(*node_idx).edges.iter() {
                let edge_type = edge.label;
                let GlobalBlockIndexAndLevel {
                    global_id: global_target,
                    level: target_level,
                } = match &final_outcome.node_to_block.mapping[edge.target] {
                    BlockAssignment::Block(block_id) => {
                        block_mapping.get(block_id).copied().unwrap()
                    }
                    BlockAssignment::Singleton(singleton_id) => {
                        singleton_mapping.get(singleton_id).copied().unwrap()
                    }
                };
                let start_time = std::cmp::max(*subject_level, target_level + 1);
                let end_time = 0; // final_state.i-1;
                inner_data_edges.push(DataEdgeAndInterval {
                    data_edge: (*global_subject, edge_type, global_target),
                    interval: (start_time, end_time),
                });
            }
            inner_data_edges.sort();
            inner_data_edges.dedup();
            for DataEdgeAndInterval {
                data_edge,
                interval,
            } in inner_data_edges.into_iter()
            {
                final_state.data_edge_callback(data_edge, interval)?;
            }
        }
    );
    print_format_last("", "\n");

    // Explicit flush is good practice, though it happens automatically on drop
    final_state.data_edge_writer_mut().flush()?;
    final_state.refines_writer_mut().flush()?;

    // Get the data edge statistics
    let data_edge_counts = final_state.data_edge_counter();
    bisimulation_statistics.add_aggregate_data_edges(data_edge_counts);

    // Serialize the bisimulation statistics
    serde_json::to_writer_pretty(statistics_file, &bisimulation_statistics)?;

    // Serialize the instrumentation statistics
    serialize_stats(output_path_buf.join("instrumentation.json"))?;

    // Emit the final outcome
    let final_node_to_block_path = output_path_buf.join("final_node_to_block");
    let final_node_to_block_file = File::create(final_node_to_block_path)?;
    let mut final_node_to_block_writer = BufWriter::new(final_node_to_block_file);
    for (node, local_block) in final_outcome.node_to_block.mapping.iter().enumerate() {
        let global_block = match local_block {
            BlockAssignment::Block(local_block_id) => block_mapping.get(local_block_id).unwrap(),
            BlockAssignment::Singleton(node_id) => singleton_mapping.get(node_id).unwrap(),
        };
        final_node_to_block_writer.write_all(&node.to_be_bytes())?;
        final_node_to_block_writer.write_all(&global_block.global_id.to_be_bytes())?;
    }
    final_node_to_block_writer.flush()?;

    Ok(())
}
