use clap::{ArgGroup, Parser};
use itertools::Itertools;
// instrument!() is used by default via #[macro_export]
use multi_summaries::instrumentation::{Stats, collector};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Error, ErrorKind, Result, Write};
use std::path::{Path, PathBuf};

// use itertools::Itertools;
use multi_summaries::graph::{EdgeType, FlatGraph, Graph, optimize_graph_for_bisimulation};

use multi_summaries::bisimulator::{
    BlockAssignment, DataEdgeCounter, FullBisimulationState, GlobalBlockIndex,
    GlobalBlockIndexAndLevel, LevelIndex, get_0_bisimulation, get_i_bisimulation,
    get_typed_0_bisimulation,
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
    max_k: Option<u64>,
}
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

    fn add_level(&mut self, bisimulation_state: &FullBisimulationState) {
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
            "After computing {:>4}-bisimulation --> Dirty blocks: {:<10}, singletons: {:<10}, blocks {:<10}, blocks (condensed) {:<10}, singletons (uncondensed) {:<10} blocks (uncondensed) {:<10}, refines edges ((un)condensed) {:<10}",
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

    fn add_aggregate_data_edges(&mut self, aggregate_counter: DataEdgeCounter) {
        assert!(self.data_edges_condensed.is_empty() && self.data_edges_uncondensed.is_empty());
        self.data_edges_condensed = aggregate_counter.condensed_counts;
        self.data_edges_uncondensed = aggregate_counter.uncondensed_counts
    }
}

#[derive(Clone)]
struct DataEdgeAndInterval {
    pub data_edge: (GlobalBlockIndex, EdgeType, GlobalBlockIndex),
    pub interval: (LevelIndex, LevelIndex),
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

    // Create graph
    let mut g = Graph::new(1_000_000_000);
    g.read_graph_parallel_memmmap(input_file, false)?;

    let flatg = optimize_graph_for_bisimulation(g);
    // Run bisimulation
    compute_bisimulation(&flatg, output_dir, min_support, type_id, max_k)?;

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
    println!("Building predecessor index...");
    let predecessors = graph.build_predecessors();

    // 2. Initial Partition: Level 0 (All nodes in one block)
    println!("Computing 0-bisimulation...");

    let zero_outcome = match type_id {
        Some(edge_type) => get_typed_0_bisimulation(graph, edge_type),
        None => get_0_bisimulation(graph),
    };
    let output_path_buf = output_dir.as_ref().to_path_buf();
    let mut bisimulation_state = FullBisimulationState::new(zero_outcome, output_path_buf.clone())?;

    let mut bisimulation_statistics = BisimulationStatistics::new();
    let statistics_path = output_path_buf.join("statistics.json");
    let statistics_file = File::create(statistics_path)?;

    // 3. Iterative Refinement
    loop {
        bisimulation_statistics.add_level(&bisimulation_state);

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
                println!("Running extra iteration to emit data edges that end at the fixed point");
                bisimulation_state =
                    get_i_bisimulation(graph, &predecessors, bisimulation_state, min_support)?;
                // bisimulation_state.shared_state.update_level()?; // TODO this call might not be needed
            }
            println!("Bisimulation stabilized at k = {}", fixed_point);
            break;
        }

        // Perform the refinement step
        bisimulation_state =
            get_i_bisimulation(graph, &predecessors, bisimulation_state, min_support)?;

        // Update state
        bisimulation_state.shared_state.update_level()?;
    }

    // 4. Emit the data edges for the remaining (non-singleton) blocks
    println!("Emitting data edges for final (non-singleton) blocks...");
    let (mut final_state, mut final_outcome) = bisimulation_state.into_parts();
    let singleton_mapping = std::mem::take(&mut final_state.singleton_mapping);
    let block_mapping = std::mem::take(&mut final_state.previous_block_mapping);
    for (block_idx, block) in std::mem::take(&mut final_outcome.blocks)
        .into_iter()
        .enumerate()
        .filter_map(|(block_idx, maybe_block)| maybe_block.map(|block| (block_idx, block)))
    {
        let GlobalBlockIndexAndLevel {
            global_id: global_subject,
            level: subject_level,
        } = block_mapping.get(&block_idx).unwrap();
        let mut sorted_inners = Vec::new();
        for node_idx in block.nodes.iter() {
            let mut inner_data_edges = Vec::new();
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
                inner_data_edges.push(DataEdgeAndInterval {
                    data_edge: (*global_subject, edge_type, *global_target),
                    interval: (start_time, end_time),
                });
            }
            inner_data_edges.sort();
            inner_data_edges.dedup();
            sorted_inners.push(inner_data_edges);
        }

        // Use a k-way merge to get the (deduplicated) union of the inner data edges
        let outer_data_edges: Vec<_> = sorted_inners.into_iter().kmerge().dedup().collect();

        for DataEdgeAndInterval {
            data_edge,
            interval,
        } in outer_data_edges.into_iter()
        {
            final_state.data_edge_callback(data_edge, interval)?;
        }
    }

    // 5. Emit the data edges for the remaining singleton blocks
    println!("Emitting data edges for final singletons...");
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
                BlockAssignment::Block(block_id) => block_mapping.get(block_id).copied().unwrap(),
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

    // Explicit flush is good practice, though it happens automatically on drop
    final_state.data_edge_writer.flush()?;
    final_state.refines_writer.flush()?;

    // Get the data edge statistics
    let data_edge_counts = final_state.data_edge_counter();
    bisimulation_statistics.add_aggregate_data_edges(data_edge_counts);

    // Serialize the bisimulation statistics
    serde_json::to_writer_pretty(statistics_file, &bisimulation_statistics)?;

    Ok(())
}
