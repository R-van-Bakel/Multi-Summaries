use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{Result, Write};

// use itertools::Itertools;
use multi_summaries::graph::{EdgeType, FlatGraph, Graph};

use multi_summaries::bisimulator::{
    BlockAssignment, FullBisimulationState, GlobalBlockIndex, GlobalBlockIndexAndLevel, LevelIndex,
    get_0_bisimulation, get_i_bisimulation,
};

fn main() -> Result<()> {
    let file_name = "fb15k.bin";
    // let file_name = "multi_block_tree.bin";
    // let file_name = "heterogeneous_hubs.bin";

    let mut g = Graph::new(1_000_000);
    g.read_graph_parallel_memmmap(&file_name, false)?;

    compute_bisimulation(&FlatGraph::new(g), 0, None)?;

    Ok(())
}

pub fn compute_bisimulation(
    graph: &FlatGraph,
    min_support: usize,
    max_k: Option<u64>,
) -> Result<()> {
    // 1. Prepare the Graph: Build the reverse index needed for dirty propagation
    println!("Building predecessor index...");
    let predecessors = graph.build_predecessors();

    // 2. Initial Partition: Level 0 (All nodes in one block)
    println!("Computing 0-bisimulation...");
    let mut bisimulation_state = FullBisimulationState::new(get_0_bisimulation(graph))?;

    // 3. Iterative Refinement
    loop {
        println!(
            "After computing {}-bisimulation (Dirty blocks: {}, singletons: {}, total blocks {})...",
            bisimulation_state.shared_state.i - 1,
            bisimulation_state.current_outcome.dirty_blocks.len(),
            bisimulation_state.current_outcome.singletons(),
            bisimulation_state.current_outcome.total_blocks()
        );

        // Break if we've reached a user-defined depth limit
        if let Some(limit) = max_k {
            if bisimulation_state.shared_state.i >= limit {
                break;
            }
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
                bisimulation_state.shared_state.update_level()?; // TODO this call might not be needed
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

        let outer_data_edges = k_way_merge(sorted_inners);
        for DataEdgeAndInterval {
            data_edge: (global_subject, edge_type, global_target),
            interval: (start_time, end_time),
        } in outer_data_edges.into_iter()
        {
            final_state.data_edge_callback((global_subject, edge_type, global_target))?;
        }
    }

    // 5. Emit the data edges for the remaining singleton blocks
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
            data_edge: (global_subject, edge_type, global_target),
            interval: (start_time, end_time),
        } in inner_data_edges.into_iter()
        {
            final_state.data_edge_callback((global_subject, edge_type, global_target))?;
        }
    }

    // Explicit flush is good practice, though it happens automatically on drop
    final_state.data_edge_writer.flush()?;
    final_state.refines_writer.flush()?;

    Ok(())
}

// TODO: double check if this is correct
pub fn k_way_merge<T: Ord + Clone>(lists: Vec<Vec<T>>) -> Vec<T> {
    let mut iters: Vec<_> = lists.into_iter().map(|v| v.into_iter()).collect();
    let mut heap: BinaryHeap<(Reverse<T>, usize)> = BinaryHeap::with_capacity(iters.len());

    // Seed heap with first element of each iterator
    for (i, iter) in iters.iter_mut().enumerate() {
        if let Some(first) = iter.next() {
            heap.push((Reverse(first), i));
        }
    }

    let mut result = Vec::new();
    let mut last_seen = None;

    while let Some((Reverse(value), idx)) = heap.pop() {
        if !last_seen.as_ref().is_some_and(|ls| ls == &value) {
            last_seen = Some(value.clone());
            result.push(value);
        }

        if let Some(next) = iters[idx].next() {
            heap.push((Reverse(next), idx));
        }
    }

    result
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
        self.data_edge.partial_cmp(&other.data_edge)
    }
}

impl Ord for DataEdgeAndInterval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data_edge.cmp(&other.data_edge)
    }
}
