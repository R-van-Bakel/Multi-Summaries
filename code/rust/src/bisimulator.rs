use fxhash::FxBuildHasher;

use crate::graph::{EdgeType, FlatGraph, NodeIndex, Predecessors}; // Assuming graph.rs is a module
use std::collections::{BTreeSet, HashMap, HashSet};

use std::fmt::{self, Display};
use std::fs::File;
use std::io::{BufWriter, Result, Write};

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
    new_mapping: HashMap<usize, i64, FxBuildHasher>,
    old_mapping: Vec<i64>,
    singleton_count: usize,
}

impl InternalNode2BlockMapper {
    pub fn new_all_zero(max_nodes: usize) -> Self {
        Self {
            new_mapping: HashMap::with_hasher(FxBuildHasher::default()), //HashMap::new(),
            old_mapping: vec![0; max_nodes],
            singleton_count: 0,
        }
    }

    pub fn new_from_previous(previous: Node2Block) -> Self {
        Self {
            new_mapping: HashMap::with_hasher(FxBuildHasher::default()),
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
pub struct SharedBisimulationState {
    pub i: u64,
    global_largest_block_id: u64,
    pub previous_block_mapping: HashMap<BlockIndex, u64>,
    pub singleton_mapping: HashMap<NodeIndex, u64>,
    refines_writer: BufWriter<File>,
    new_mappings: HashMap<NodeIndex, u64>,
    to_be_removed_local_ids: HashSet<NodeIndex>,
}

impl SharedBisimulationState {
    fn new(bisimulation_outcome: &KBisimulationOutcome) -> Result<Self> {
        let i = 1; // TODO NB the current code sets this to 0u64 initially

        let mut previous_block_mapping = HashMap::new();
        for j in 0..bisimulation_outcome.blocks.len() {
            previous_block_mapping.insert(j, j as u64);
        }

        let singleton_mapping = HashMap::new();

        let global_largest_block_id = (bisimulation_outcome.blocks.len() - 1) as u64;

        let file = File::create(format!("refines/refines_{}", i))?;
        let refines_writer = BufWriter::new(file);

        let new_mappings: HashMap<NodeIndex, u64> = HashMap::new();
        let to_be_removed_local_ids: HashSet<NodeIndex> = HashSet::new();

        Ok(Self {
            i,
            global_largest_block_id,
            previous_block_mapping: previous_block_mapping.into(),
            singleton_mapping: singleton_mapping.into(),
            refines_writer: refines_writer.into(),
            new_mappings: new_mappings.into(),
            to_be_removed_local_ids: to_be_removed_local_ids.into(),
        })
    }

    pub fn update_level(&mut self) -> Result<()> {
        for to_be_removed_local_id in self.to_be_removed_local_ids.drain() {
            self.previous_block_mapping.remove(&to_be_removed_local_id);
        }

        self.previous_block_mapping
            .extend(self.new_mappings.drain());

        self.i += 1;

        let file = File::create(format!("refines/refines_{}", self.i))?;
        self.refines_writer = BufWriter::new(file);

        Ok(())
    }

    fn refine_callback(
        &mut self,
        refine_source_block: &BlockAssignment,
        refine_target_block: &BlockAssignment,
    ) -> Result<()> {
        match (refine_source_block, refine_target_block) {
            (BlockAssignment::Block(source_local), BlockAssignment::Block(target_local)) => {
                self.global_largest_block_id += 1;
                let source_global = self.global_largest_block_id;

                let target_global = self.previous_block_mapping.get(target_local).unwrap();

                //println!("{} -> {}", source_global, target_global);
                self.refines_writer
                    .write_all(&source_global.to_be_bytes())?;
                self.refines_writer
                    .write_all(&target_global.to_be_bytes())?;

                self.new_mappings.insert(*source_local, source_global);
            }
            (BlockAssignment::Singleton(node_index), BlockAssignment::Block(target_local)) => {
                //let source_global = (-(*source_local as i64)) - 1;
                self.global_largest_block_id += 1;
                let source_global = self.global_largest_block_id;

                let target_global = self.previous_block_mapping.get(target_local).unwrap();

                // println!("s - {} -> {}", source_global, target_global);
                self.refines_writer
                    .write_all(&source_global.to_be_bytes())?;
                self.refines_writer
                    .write_all(&target_global.to_be_bytes())?;

                self.singleton_mapping.insert(*node_index, source_global);
                // TODO: this can also be written out immediately
            }
            _ => {
                panic!("Not a valid edge");
            }
        }
        Ok(())
    }

    fn refine_target_can_be_freed(&mut self, local_target_id: &BlockIndex) -> Result<()> {
        self.to_be_removed_local_ids.insert(*local_target_id);
        Ok(())
    }
}

pub struct FullBisimulationState {
    pub shared_state: SharedBisimulationState,
    pub current_outcome: KBisimulationOutcome,
}

impl FullBisimulationState {
    pub fn new(bisimulation_outcome: KBisimulationOutcome) -> Result<Self> {
        Ok(Self {
            shared_state: SharedBisimulationState::new(&bisimulation_outcome)?,
            current_outcome: bisimulation_outcome.into(),
        })
    }

    fn steal_outcome(self) -> (PartialBisimulationState, KBisimulationOutcome) {
        let FullBisimulationState {
            shared_state,
            current_outcome,
        } = self;
        (PartialBisimulationState { shared_state }, current_outcome)
    }

    pub fn into_parts(self) -> (SharedBisimulationState, KBisimulationOutcome) {
        let FullBisimulationState {
            shared_state,
            current_outcome,
        } = self;
        (shared_state, current_outcome)
    }
}

struct PartialBisimulationState {
    shared_state: SharedBisimulationState,
}

impl PartialBisimulationState {
    fn restore_outcome(self, new_outcome: KBisimulationOutcome) -> FullBisimulationState {
        FullBisimulationState {
            shared_state: self.shared_state,
            current_outcome: new_outcome,
        }
    }
}

pub fn get_i_bisimulation(
    graph: &FlatGraph,
    predecessors: &Predecessors, // the predecessors computed with graph.build_predecessors()
    // We take ownership of the previous outcome and will reuse parts of this for the current outcome
    bisimulation_state: FullBisimulationState,
    min_support: usize,
) -> Result<FullBisimulationState> {
    let (mut partial_bisimulation_state, prev_outcome) = bisimulation_state.steal_outcome();
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
            // We use a BtreeSet instead of using unique and then sorted on the iterator.
            // This reduced runtime by 10-20% in experiments with the lubm dataset.
            let btsig: BTreeSet<_> = graph
                .get_node(v)
                .edges
                .iter()
                .map(|e| {
                    (
                        e.label,
                        this_level_mapper.get_previous_level_block_idx(e.target),
                    )
                })
                .collect();
            let sig: Vec<(EdgeType, i64)> = btsig.into_iter().collect();

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
                partial_bisimulation_state
                    .shared_state
                    .refine_callback(&refines_subject, &refines_object)?;
            } else {
                only_singletons = false;
                let new_block = Some(Block {
                    nodes,
                    f: partial_bisimulation_state.shared_state.i,
                });

                let target_idx = if let Some(free_idx) = freeblock_indices.pop() {
                    k_blocks[free_idx] = new_block;
                    free_idx
                } else {
                    k_blocks.push(new_block);
                    k_blocks.len() - 1
                };

                let refines_subject = BlockAssignment::Block(target_idx);
                partial_bisimulation_state
                    .shared_state
                    .refine_callback(&refines_subject, &refines_object)?;

                // we just inserted it, so it must exist.
                for &node in k_blocks[target_idx].as_ref().unwrap().nodes.iter() {
                    this_level_mapper.overwrite_mapping(node, target_idx);
                }
            }
        }
        if only_singletons {
            partial_bisimulation_state
                .shared_state
                .refine_target_can_be_freed(&dirty_idx)?;
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

    let full_bisimulation_state =
        partial_bisimulation_state.restore_outcome(KBisimulationOutcome {
            blocks: k_blocks,
            dirty_blocks: dirty_blocks,
            node_to_block: this_level_mapper.commit_new_mapping(),
            freeblock_indices: freeblock_indices,
        });

    Ok(full_bisimulation_state)
}

pub fn get_typed_0_bisimulation(graph: &FlatGraph, rdf_type_id: EdgeType) -> KBisimulationOutcome {
    let mut partition_map: HashMap<Vec<NodeIndex>, Vec<NodeIndex>> = HashMap::new();

    for node_idx in 0..graph.get_size() {
        let node = graph.get_node(node_idx);
        let mut type_set: HashSet<usize> = HashSet::new();
        for edge in node.edges {
            if edge.label == rdf_type_id {
                type_set.insert(edge.target);
            }
        }
        let types: Vec<NodeIndex> = type_set.into_iter().collect();
        partition_map.entry(types).or_default().push(node_idx);
    }

    let mut new_blocks = Vec::new();
    let mut mapper = InternalNode2BlockMapper::new_all_zero(graph.get_size());
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

pub fn get_0_bisimulation(graph: &FlatGraph) -> KBisimulationOutcome {
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
