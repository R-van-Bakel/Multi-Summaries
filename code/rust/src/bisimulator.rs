use itertools::Itertools;

use crate::graph::{EdgeType, Graph, NodeIndex, Predecessors}; // Assuming graph.rs is a module
use std::collections::{HashMap, HashSet};

use std::fmt::{self, Display};
use std::io::Result;
use std::sync::Arc;

pub type BlockIndex = usize;

// C++ uses negative numbers for singletons.
// In Rust, an Enum is more idiomatic and type-safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockAssignment {
    Block(BlockIndex),
    Singleton(NodeIndex),
}

impl Display for BlockAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockAssignment::Block(id) => write!(f, "{}", id)?,
            BlockAssignment::Singleton(id) => write!(f, "s-{}", id)?,
        }
        Ok(())
    }
}

pub struct Block {
    // The nodes in this block
    pub nodes: Vec<NodeIndex>,
    // The earliest level at which this block was encountered
    pub f: u64,
}
pub type BlockPtr = Arc<Block>;

pub struct Node2BlockMapper {
    // Stores either BlockIndex (as positive) or NodeIndex (as negative/encoded)
    // We'll follow the C++ logic: mapping[i] >= 0 is BlockIndex, < 0 is Singleton
    mapping: Vec<i64>,
    singleton_count: usize,
    pub freeblock_indices: Vec<BlockIndex>,
}

impl Node2BlockMapper {
    pub fn new_all_zero(max_nodes: usize) -> Self {
        Self {
            mapping: vec![0; max_nodes],
            singleton_count: 0,
            freeblock_indices: Vec::new(),
        }
    }

    pub fn get_block(&self, node: NodeIndex) -> i64 {
        self.mapping[node]
    }

    pub fn put_into_singleton(&mut self, node: NodeIndex) {
        // C++: node_to_block[node] = -((block_or_singleton_index)node) - 1;
        if self.mapping[node] < 0 {
            panic!("Node is already a singleton");
        }
        self.singleton_count += 1;
        self.mapping[node] = -(node as i64) - 1;
    }

    pub fn overwrite_mapping(&mut self, node: NodeIndex, block: BlockIndex) {
        self.mapping[node] = block as i64;
    }
}

pub struct KBisimulationOutcome {
    pub blocks: Vec<BlockPtr>,
    pub dirty_blocks: HashSet<BlockIndex>,
    pub node_to_block: Node2BlockMapper,
}

impl KBisimulationOutcome {
    pub fn total_blocks(&self) -> usize {
        let non_singleton = self.blocks.iter().filter(|b| !b.nodes.is_empty()).count();
        self.node_to_block.singleton_count + non_singleton
    }
    pub fn singletons(&self) -> usize {
        return self.node_to_block.singleton_count;
    }
}

pub fn get_i_bisimulation<F, F2>(
    graph: &Graph,
    predecessors: &Predecessors, // the predecessors computed with graph.build_predecessors()
    prev_outcome: &KBisimulationOutcome,
    i: u64,
    min_support: usize,
    mut refine_callback: F,
    mut refine_target_can_be_freed: F2,
) -> Result<KBisimulationOutcome>
where
    F: FnMut(&BlockAssignment, &BlockAssignment) -> Result<()>,
    F2: FnMut(&BlockIndex) -> Result<()>,
{
    let mut k_blocks = prev_outcome.blocks.clone();
    let mut k_mapper = Node2BlockMapper {
        mapping: prev_outcome.node_to_block.mapping.clone(),
        singleton_count: prev_outcome.node_to_block.singleton_count,
        freeblock_indices: prev_outcome.node_to_block.freeblock_indices.clone(),
    };

    let mut refined_block_set: Vec<BlockIndex> = Vec::new();

    // Iterate through dirty blocks from the previous step
    for &dirty_idx in &prev_outcome.dirty_blocks {
        let block = &prev_outcome.blocks[dirty_idx];
        if block.nodes.len() <= min_support {
            continue;
        }

        // signature_t: Map of (EdgeLabel, TargetBlockID) -> Nodes
        let mut signatures: HashMap<Vec<(EdgeType, i64)>, Vec<NodeIndex>> = HashMap::new();

        for &v in block.nodes.iter() {
            let sig: Vec<(EdgeType, i64)> = graph.nodes[v]
                .edges
                .iter()
                .map(|e| (e.label, prev_outcome.node_to_block.get_block(e.target)))
                .unique()
                // Sort signature to ensure hash consistency
                .sorted()
                .collect();

            signatures.entry(sig).or_default().push(v);
        }

        if signatures.len() <= 1 {
            continue;
        } // No split occurred

        refined_block_set.push(dirty_idx);

        // Mark old block as free
        k_mapper.freeblock_indices.push(dirty_idx);
        k_blocks[dirty_idx] = Arc::new(Block {
            nodes: Vec::new(),
            f: 0,
        });

        let refines_object: BlockAssignment = BlockAssignment::Block(dirty_idx);

        let mut only_singletons = true;

        for (_, nodes) in signatures {
            if nodes.len() == 1 {
                k_mapper.put_into_singleton(nodes[0]);
                let refines_subject: BlockAssignment = BlockAssignment::Singleton(nodes[0]);
                refine_callback(&refines_subject, &refines_object)?;
            } else {
                only_singletons = false;
                let new_block_ptr = Arc::new(Block { nodes, f: i });
                let target_idx = if let Some(free_idx) = k_mapper.freeblock_indices.pop() {
                    k_blocks[free_idx] = new_block_ptr;
                    free_idx
                } else {
                    k_blocks.push(new_block_ptr);
                    k_blocks.len() - 1
                };

                let refines_subject = BlockAssignment::Block(target_idx);
                refine_callback(&refines_subject, &refines_object)?;
                for &node in k_blocks[target_idx].nodes.iter() {
                    k_mapper.overwrite_mapping(node, target_idx);
                }
            }
        }
        if only_singletons {
            refine_target_can_be_freed(&dirty_idx)?;
        }
    }

    // --- Dirty Block Propagation ---
    // Note: This requires a reverse graph (predecessors).
    // If your Graph doesn't store 'reverse', you'll need to compute it once.
    let mut next_dirty = HashSet::new();

    // Mark nodes for dirty propagation
    // for &v in block.nodes.iter() {
    //     nodes_from_split_blocks.insert(v);
    // }

    // Mark blocks as dirty if they point to nodes that were part of a split
    for refinded_block_idx in refined_block_set {
        let block = &prev_outcome.blocks[refinded_block_idx];
        for target in block.nodes.iter() {
            // there must be a predecessor list, it might be empty.
            let preds = predecessors.get(*target).unwrap();
            for &source in preds {
                let dirty_block_id = k_mapper.get_block(source);

                // If it is a singleton, it can never split, so no need to mark
                if dirty_block_id < 0 {
                    continue;
                }

                let block_idx = dirty_block_id as usize;

                // Only mark if the block size meets the min_support requirement
                if k_blocks[block_idx].nodes.len() >= min_support {
                    next_dirty.insert(block_idx);
                }
            }
        }
    }
    Ok(KBisimulationOutcome {
        blocks: k_blocks,
        dirty_blocks: next_dirty,
        node_to_block: k_mapper,
    })
}

pub fn get_typed_0_bisimulation(graph: &Graph, rdf_type_id: EdgeType) -> KBisimulationOutcome {
    let mut partition_map: HashMap<Vec<NodeIndex>, Vec<NodeIndex>> = HashMap::new();

    for (i, node) in graph.nodes.iter().enumerate() {
        let mut type_set: HashSet<usize> = HashSet::new();
        for edge in &node.edges {
            if edge.label == rdf_type_id {
                type_set.insert(edge.target);
            }
        }
        let types: Vec<NodeIndex> = type_set.into_iter().collect();
        partition_map.entry(types).or_default().push(i);
    }

    let mut new_blocks = Vec::new();
    let mut mapper = Node2BlockMapper::new_all_zero(graph.nodes.len());
    let mut dirty = HashSet::new();

    for (_types, nodes) in partition_map {
        if nodes.len() == 1 {
            mapper.put_into_singleton(nodes[0]);
        } else {
            let block_idx = new_blocks.len();
            for &node_idx in &nodes {
                mapper.overwrite_mapping(node_idx, block_idx);
            }
            new_blocks.push(Arc::new(Block { nodes, f: 0 }));
            dirty.insert(block_idx);
        }
    }

    KBisimulationOutcome {
        blocks: new_blocks,
        dirty_blocks: dirty,
        node_to_block: mapper,
    }
}

pub fn get_0_bisimulation(graph: &crate::graph::Graph) -> KBisimulationOutcome {
    let node_count = graph.get_size();

    // Create the initial block containing all node indices
    // C++: block->reserve(amount); for (unsigned int i = 0; i < amount; i++) { block->emplace_back(i); }
    let initial_block: Vec<usize> = (0..node_count).collect();
    let mut blocks = Vec::new();
    blocks.push(Arc::new(Block {
        nodes: initial_block,
        f: 0,
    }));

    // Initialize the mapper where every node points to block index 0
    // C++: std::shared_ptr<AllToZeroNode2BlockMapper> node_to_block = ...
    let mapper = Node2BlockMapper::new_all_zero(node_count);

    // Set the first block as dirty to trigger the first round of refinement
    // C++: dirty.set_dirty(0);
    let mut dirty_blocks = std::collections::HashSet::new();
    if node_count > 0 {
        dirty_blocks.insert(0);
    }

    KBisimulationOutcome {
        blocks,
        dirty_blocks,
        node_to_block: mapper,
    }
}
