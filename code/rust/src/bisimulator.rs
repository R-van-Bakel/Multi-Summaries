use itertools::Itertools;

use crate::graph::{EdgeType, Graph, NodeIndex, Predecessors}; // Assuming graph.rs is a module
use std::collections::{HashMap, HashSet};

use std::fmt::{self, Display};
use std::io::Result;

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

pub struct Node2Block {
    pub mapping: Vec<i64>,
    pub singleton_count: usize,
}

impl Node2Block {
    fn new_all_zero(max_nodes: usize) -> Self {
        Self {
            mapping: vec![0; max_nodes],
            singleton_count: 0,
        }
    }
}

struct InternalNode2BlockMapper {
    // Stores either BlockIndex (as positive) or NodeIndex (as negative/encoded)
    // mapping[i] >= 0 is BlockIndex, < 0 is Singleton

    // The new_mapping is taking precedence over what is in the old mapping.
    new_mapping: HashMap<usize, i64>,
    old_mapping: Vec<i64>,
    singleton_count: usize,
}

impl InternalNode2BlockMapper {
    pub fn new_all_zero(max_nodes: usize) -> Self {
        Self {
            new_mapping: HashMap::new(),
            old_mapping: vec![0; max_nodes],
            singleton_count: 0,
        }
    }

    pub fn new_from_previous(previous: Node2Block) -> Self {
        Self {
            new_mapping: HashMap::new(),
            old_mapping: previous.mapping,
            singleton_count: previous.singleton_count,
        }
    }

    // all modifications to this object are done on the new_mapping, such that old_mapping can still be used as long as it is needed.
    // when that is no longer needed, we can finalize the mapping by committing which will write all changes in new_mapping into the old_mapping
    // This consumes the current object, and returns the committed mapping and the singleton count
    pub fn commit_new_mapping(mut self) -> Node2Block {
        for (k, v) in self.new_mapping.into_iter() {
            self.old_mapping[k] = v;
        }
        //note: because of into_iter, the new_mapping is already empty at this point.
        Node2Block {
            mapping: self.old_mapping,
            singleton_count: self.singleton_count,
        }
    }

    pub fn get_previous_level_block_idx(&self, node: NodeIndex) -> i64 {
        return self.old_mapping[node];
    }

    pub fn get_block_idx(&self, node: NodeIndex) -> i64 {
        if let Some(index) = self.new_mapping.get(&node) {
            return *index;
        } else {
            return self.old_mapping[node];
        }
    }

    pub fn put_into_singleton(&mut self, node: NodeIndex) {
        if self.get_block_idx(node) < 0 {
            panic!("Node is already a singleton");
        }
        self.singleton_count += 1;
        self.new_mapping.insert(node, -(node as i64) - 1);
    }

    pub fn overwrite_mapping(&mut self, node: NodeIndex, block: BlockIndex) {
        self.new_mapping.insert(node, block as i64);
    }
}

pub struct KBisimulationOutcome {
    pub blocks: Vec<Option<Block>>,
    // A list of all blocks in blocks that are None.
    pub freeblock_indices: Vec<BlockIndex>,
    pub dirty_blocks: Vec<BlockIndex>,
    pub node_to_block: Node2Block,
}

impl KBisimulationOutcome {
    pub fn total_blocks(&self) -> usize {
        let non_singleton = self
            .blocks
            .iter()
            .filter(|b| match b {
                Some(b) => {
                    // There is an invariant here that could be exploited. There must never be an empty block that is not None.
                    // Adding this sanity check here to check the invariant.
                    if b.nodes.is_empty() {
                        panic!("A block must never be empty");
                    }
                    true
                }
                None => false,
            })
            .count();
        self.node_to_block.singleton_count + non_singleton
    }
    pub fn singletons(&self) -> usize {
        return self.node_to_block.singleton_count;
    }
}

pub fn get_i_bisimulation<F, F2>(
    graph: &Graph,
    predecessors: &Predecessors, // the predecessors computed with graph.build_predecessors()
    // We take ownership of the previous outcome and will reuse parts of this for the current outcome
    prev_outcome: KBisimulationOutcome,
    i: u64,
    min_support: usize,
    mut refine_callback: F,
    mut refine_target_can_be_freed: F2,
) -> Result<KBisimulationOutcome>
where
    F: FnMut(&BlockAssignment, &BlockAssignment) -> Result<()>,
    F2: FnMut(&BlockIndex) -> Result<()>,
{
    // We take the parts out of the previous_outcome for reuse
    let mut k_blocks = prev_outcome.blocks;

    let mut dirty_blocks = prev_outcome.dirty_blocks;

    let mut this_level_mapper =
        InternalNode2BlockMapper::new_from_previous(prev_outcome.node_to_block);

    let mut freeblock_indices = prev_outcome.freeblock_indices;

    let mut refined_block_set: Vec<Block> = Vec::new();

    // Iterate through dirty blocks from the previous step
    for dirty_idx in dirty_blocks.iter() {
        // we are sure this block must exist, so we can unwrap
        let block_ref = k_blocks[*dirty_idx].as_ref().unwrap();
        if block_ref.nodes.len() <= min_support {
            continue;
        }

        // signature_t: Map of (EdgeLabel, TargetBlockID) -> Nodes
        let mut signatures: HashMap<Vec<(EdgeType, i64)>, Vec<NodeIndex>> = HashMap::new();

        for &v in block_ref.nodes.iter() {
            let sig: Vec<(EdgeType, i64)> = graph.nodes[v]
                .edges
                .iter()
                .map(|e| {
                    (
                        e.label,
                        this_level_mapper.get_previous_level_block_idx(e.target),
                    )
                })
                .unique()
                // Sort signature to ensure hash consistency
                .sorted()
                .collect();

            signatures.entry(sig).or_default().push(v);
        }

        if signatures.len() <= 1 {
            continue;
        } // No split occurred

        // We take ownership of the block and put a None at that spot in k_block, and mark that block as free
        let block = std::mem::replace(&mut k_blocks[*dirty_idx], None).unwrap();
        freeblock_indices.push(*dirty_idx);

        let refines_object: BlockAssignment = BlockAssignment::Block(*dirty_idx);

        let mut only_singletons = true;

        for (_, nodes) in signatures.into_iter() {
            if nodes.len() == 1 {
                this_level_mapper.put_into_singleton(nodes[0]);
                let refines_subject: BlockAssignment = BlockAssignment::Singleton(nodes[0]);
                refine_callback(&refines_subject, &refines_object)?;
            } else {
                only_singletons = false;
                let new_block = Some(Block { nodes, f: i });

                let target_idx = if let Some(free_idx) = freeblock_indices.pop() {
                    k_blocks[free_idx] = new_block;
                    free_idx
                } else {
                    k_blocks.push(new_block);
                    k_blocks.len() - 1
                };

                let refines_subject = BlockAssignment::Block(target_idx);
                refine_callback(&refines_subject, &refines_object)?;

                // we just inserted it, so it must exist.
                for &node in k_blocks[target_idx].as_ref().unwrap().nodes.iter() {
                    this_level_mapper.overwrite_mapping(node, target_idx);
                }
            }
        }
        if only_singletons {
            refine_target_can_be_freed(&dirty_idx)?;
        }

        refined_block_set.push(block);
    }

    // --- Dirty Block Propagation, we reuse the old dirty blocks memory ---
    dirty_blocks.clear();

    // Mark blocks as dirty if they point to nodes that were part of a split
    for refined_block in refined_block_set {
        for target in refined_block.nodes.iter() {
            // there must be a predecessor list, it might be None
            let maybe_preds = predecessors.get(*target).unwrap();
            match maybe_preds {
                None => {
                    continue;
                }
                Some(preds) => {
                    for &source in preds {
                        let dirty_block_id = this_level_mapper.get_block_idx(source);

                        // If it is a singleton, it can never split, so no need to mark
                        if dirty_block_id < 0 {
                            continue;
                        }

                        let block_idx = dirty_block_id as usize;

                        // Only mark if the block size meets the min_support requirement
                        if k_blocks[block_idx].as_ref().unwrap().nodes.len() >= min_support {
                            // only add it if it is not a duplicate, this is a heuristic saving by checking whether it is the same as the previous
                            if let Some(last) = dirty_blocks.last() {
                                if *last == block_idx {
                                    continue;
                                }
                            }
                            dirty_blocks.push(block_idx);
                        }
                    }
                }
            }
        }
    }

    dirty_blocks.sort();
    dirty_blocks.dedup();
    // it is likely that each next level dirty block vector has fewer elements, hence shrinking
    dirty_blocks.shrink_to_fit();

    Ok(KBisimulationOutcome {
        blocks: k_blocks,
        dirty_blocks: dirty_blocks,
        node_to_block: this_level_mapper.commit_new_mapping(),
        freeblock_indices: freeblock_indices,
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
    let mut mapper = InternalNode2BlockMapper::new_all_zero(graph.nodes.len());
    let mut dirty = Vec::new();

    for (_types, nodes) in partition_map {
        if nodes.len() == 1 {
            mapper.put_into_singleton(nodes[0]);
        } else {
            let block_idx = new_blocks.len();
            for &node_idx in &nodes {
                mapper.overwrite_mapping(node_idx, block_idx);
            }
            new_blocks.push(Some(Block { nodes, f: 0 }));
            dirty.push(block_idx);
        }
    }

    KBisimulationOutcome {
        blocks: new_blocks,
        dirty_blocks: dirty,
        node_to_block: mapper.commit_new_mapping(),
        freeblock_indices: Vec::new(),
    }
}

pub fn get_0_bisimulation(graph: &crate::graph::Graph) -> KBisimulationOutcome {
    let node_count = graph.get_size();

    // Create the initial block containing all node indices
    // C++: block->reserve(amount); for (unsigned int i = 0; i < amount; i++) { block->emplace_back(i); }
    let initial_block: Vec<usize> = (0..node_count).collect();
    let mut blocks = Vec::new();
    blocks.push(Some(Block {
        nodes: initial_block,
        f: 0,
    }));

    // Initialize the mapper where every node points to block index 0
    // C++: std::shared_ptr<AllToZeroNode2BlockMapper> node_to_block = ...
    let mapper = Node2Block::new_all_zero(node_count);

    // Set the first block as dirty to trigger the first round of refinement
    // C++: dirty.set_dirty(0);
    let mut dirty_blocks = Vec::new();
    if node_count > 0 {
        dirty_blocks.push(0);
    }

    KBisimulationOutcome {
        blocks,
        dirty_blocks,
        node_to_block: mapper,
        freeblock_indices: Vec::new(),
    }
}
