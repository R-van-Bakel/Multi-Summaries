use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Result, Write};

use itertools::Itertools;
use multi_summaries::graph::{Graph, NodeIndex};

use multi_summaries::bisimulator::{
    BlockAssignment, BlockIndex, get_0_bisimulation, get_i_bisimulation,
};

fn main() -> Result<()> {
    let file_name = "fb15k.bin";

    let mut g = Graph::new(1_000_000);
    g.read_graph_parallel_memmmap(&file_name, false)?;
    compute_bisimulation(&g, 0, None)?;

    Ok(())
}

pub fn compute_bisimulation(graph: &Graph, min_support: usize, max_k: Option<u64>) -> Result<()> {
    // 1. Prepare the Graph: Build the reverse index needed for dirty propagation
    println!("Building predecessor index...");
    let predecessors = graph.build_predecessors();

    // 2. Initial Partition: Level 0 (All nodes in one block)
    println!("Computing 0-bisimulation...");
    let mut current_outcome = get_0_bisimulation(graph);

    // The current level of simulation
    let mut i = 0u64;

    let mut previous_block_mapping: HashMap<BlockIndex, u64> = HashMap::new();
    for i in 0..current_outcome.blocks.len() {
        previous_block_mapping.insert(i, i as u64);
    }

    let mut singleton_mapping: HashMap<NodeIndex, u64> = HashMap::new();

    let mut global_largest_block_id = (current_outcome.blocks.len() - 1) as u64;

    // 3. Iterative Refinement
    loop {
        println!(
            "After computing {}-bisimulation (Dirty blocks: {}, singletons: {}, total blocks {})...",
            i,
            current_outcome.dirty_blocks.len(),
            current_outcome.singletons(),
            current_outcome.total_blocks()
        );

        // Break if we've reached a user-defined depth limit
        if let Some(limit) = max_k {
            if i >= limit {
                break;
            }
        }

        // If no blocks are dirty, the partition is stable
        if current_outcome.dirty_blocks.is_empty() {
            println!("Bisimulation stabilized at k = {}", i);
            break;
        }

        i += 1;

        let file = File::create(format!("refines/refines_{}", i))?;
        let mut refines_writer = BufWriter::new(file);

        let mut new_mappings: HashMap<NodeIndex, u64> = HashMap::new();
        let mut to_be_removed_local_ids: HashSet<NodeIndex> = HashSet::new();

        let refine_callback =
            |refine_source_block: &BlockAssignment, refine_target_block: &BlockAssignment| {
                match (refine_source_block, refine_target_block) {
                    (
                        BlockAssignment::Block(source_local),
                        BlockAssignment::Block(target_local),
                    ) => {
                        global_largest_block_id += 1;
                        let source_global = global_largest_block_id;

                        let target_global = previous_block_mapping.get(target_local).unwrap();

                        //println!("{} -> {}", source_global, target_global);
                        refines_writer.write_all(&source_global.to_be_bytes())?;
                        refines_writer.write_all(&target_global.to_be_bytes())?;

                        new_mappings.insert(*source_local, source_global);
                    }
                    (
                        BlockAssignment::Singleton(node_index),
                        BlockAssignment::Block(target_local),
                    ) => {
                        //let source_global = (-(*source_local as i64)) - 1;
                        global_largest_block_id += 1;
                        let source_global = global_largest_block_id;

                        let target_global = previous_block_mapping.get(target_local).unwrap();

                        // println!("s - {} -> {}", source_global, target_global);
                        refines_writer.write_all(&source_global.to_be_bytes())?;
                        refines_writer.write_all(&target_global.to_be_bytes())?;

                        singleton_mapping.insert(*node_index, source_global);
                        // TODO: this can also be written out immediately
                    }
                    _ => {
                        panic!("Not a valid edge");
                    }
                }
                Ok(())
            };

        let refine_target_can_be_freed = |local_target_id: &BlockIndex| {
            to_be_removed_local_ids.insert(*local_target_id);
            Ok(())
        };

        // Perform the refinement step
        current_outcome = get_i_bisimulation(
            graph,
            &predecessors,
            &current_outcome,
            i,
            min_support,
            refine_callback,
            refine_target_can_be_freed,
        )?;

        for to_be_removed_local_id in to_be_removed_local_ids {
            previous_block_mapping.remove(&to_be_removed_local_id);
        }

        for (local, global) in new_mappings.iter() {
            previous_block_mapping.insert(*local, *global);
        }
    }

    let mut node_index_to_global_terminal_block_id = vec![0; graph.get_size()];

    for (block_index, block) in current_outcome.blocks.iter().enumerate() {
        if block.nodes.len() == 0 {
            continue;
        }
        let global_block_id = previous_block_mapping.get(&block_index).unwrap();
        for node in &*(*block).nodes {
            node_index_to_global_terminal_block_id[*node] = *global_block_id;
        }
    }

    for (block_index, global_block_id) in singleton_mapping {
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
