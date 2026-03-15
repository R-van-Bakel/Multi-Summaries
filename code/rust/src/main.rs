use std::fs::File;
use std::io::{BufWriter, Result, Write};

use itertools::Itertools;
use multi_summaries::graph::{Graph, FlatGraph};

use multi_summaries::bisimulator::{FullBisimulationState, get_0_bisimulation, get_i_bisimulation};

fn main() -> Result<()> {
    let file_name = "lubm.bin";

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

        // If no blocks are dirty, the partition is stable
        if bisimulation_state.current_outcome.dirty_blocks.is_empty() {
            println!(
                "Bisimulation stabilized at k = {}",
                bisimulation_state.shared_state.i - 1
            );
            break;
        }

        // Perform the refinement step
        bisimulation_state =
            get_i_bisimulation(graph, &predecessors, bisimulation_state, min_support)?;

        // Update state
        bisimulation_state.shared_state.update_level()?;
    }

    // Deconstruct the bisimulation state
    let (final_state, final_outcome) = bisimulation_state.into_parts();

    let mut node_index_to_global_terminal_block_id = vec![0; graph.get_size()];

    for (block_index, maybe_block) in final_outcome.blocks.iter().enumerate() {
        match maybe_block {
            None => continue,
            Some(block) => {
                if block.nodes.len() == 0 {
                    panic!("This must never happen");
                }
                let global_block_id = final_state
                    .previous_block_mapping
                    .get(&block_index)
                    .unwrap();
                for node in &*(*block).nodes {
                    node_index_to_global_terminal_block_id[*node] = *global_block_id;
                }
            }
        }
    }

    // NB cannot use `state` here as singleton_mapping is taken from
    for (block_index, global_block_id) in final_state.singleton_mapping {
        node_index_to_global_terminal_block_id[block_index] = global_block_id;
    }

    println!("{}", node_index_to_global_terminal_block_id.len());
    println!(
        "{}",
        node_index_to_global_terminal_block_id
            .iter()
            .unique()
            .collect::<Vec<_>>()
            .len()
    );

    let file = File::create("node_index_to_global_terminal_block_id")?;
    let mut writer = BufWriter::new(file);

    for num in node_index_to_global_terminal_block_id {
        // to_be_bytes() converts the u64 into an [u8; 8] array in Big Endian
        writer.write_all(&num.to_be_bytes())?;
    }

    // Explicit flush is good practice, though it happens automatically on drop
    writer.flush()?;

    Ok(())
}
