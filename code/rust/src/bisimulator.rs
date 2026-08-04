use fxhash::{FxHashMap, FxHashSet};
use itertools::Itertools;

use crate::graph::{EdgeType, FlatGraph, NodeIndex, Predecessors};
use std::collections::BTreeSet;

use std::fmt::{self, Display};
// use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{Result, Write};
// use std::io::{BufWriter, Result, Write};
// use std::path::{Path, PathBuf};

pub type BlockIndex = usize;
pub type GlobalBlockIndex = usize;
pub type LevelIndex = u64;

// C++ uses negative numbers for singletons.
// In Rust, an Enum is more idiomatic and type-safe.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockAssignment {
    Block(BlockIndex),
    Singleton(NodeIndex),
}

impl BlockAssignment {
    const VARIANT1_HASH: usize = 12634128529936681850_usize; // 8-byte slice from SHA256(0), truncates for 32-bit systems
    const VARIANT2_HASH: usize = 14782610670539863730_usize; // 8-byte slice from SHA256(1), truncates for 32-bit systems
    fn salt(&self) -> usize {
        match self {
            BlockAssignment::Block(v) => v ^ BlockAssignment::VARIANT1_HASH,
            BlockAssignment::Singleton(v) => v ^ BlockAssignment::VARIANT2_HASH,
        }
    }
}

impl Hash for BlockAssignment {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.salt().hash(state);
    }
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
    pub f: LevelIndex,
}

pub struct Node2Block {
    pub mapping: Vec<BlockAssignment>,
    pub singleton_count: NodeIndex,
}

impl Node2Block {
    fn new_all_zero(max_nodes: NodeIndex) -> Self {
        // NB: BlockAssignment::Block(0) it technically semantically incorrect if there is only a singel vertex in the graph (then block Assignment::Singleton(0) would be better)
        Self {
            mapping: vec![BlockAssignment::Block(0); max_nodes],
            singleton_count: 0,
        }
    }
}

struct InternalNode2BlockMapper {
    // Stores either BlockIndex (as positive) or NodeIndex (as negative/encoded)
    // mapping[i] >= 0 is BlockIndex, < 0 is Singleton

    // The new_mapping is taking precedence over what is in the old mapping.
    new_mapping: FxHashMap<usize, BlockAssignment>,
    old_mapping: Vec<BlockAssignment>,
    singleton_count: usize,
}

impl InternalNode2BlockMapper {
    pub fn new_all_zero(max_nodes: usize) -> Self {
        // NB: BlockAssignment::Block(0) it technically semantically incorrect if there is only a single vertex in the graph (then block Assignment::Singleton(0) would be better)
        Self {
            new_mapping: FxHashMap::default(),
            old_mapping: vec![BlockAssignment::Block(0); max_nodes],
            singleton_count: 0,
        }
    }

    pub fn new_from_previous(previous: Node2Block) -> Self {
        Self {
            new_mapping: FxHashMap::default(),
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

    pub fn get_previous_level_block_idx(&self, node: NodeIndex) -> BlockAssignment {
        self.old_mapping[node].clone()
    }

    pub fn get_block_idx(&self, node: NodeIndex) -> BlockAssignment {
        if let Some(index) = self.new_mapping.get(&node) {
            (*index).clone()
        } else {
            self.old_mapping[node].clone()
        }
    }

    // pub fn put_into_singleton(&mut self, node: NodeIndex) {
    //     assert!(
    //         matches!(self.get_block_idx(node), BlockAssignment::Singleton(_)),
    //         "Node is already a singleton"
    //     );

    //     // if self.get_block_idx(node) < 0 {
    //     //     panic!();
    //     // }
    //     self.singleton_count += 1;
    //     self.new_mapping.insert(node, BlockAssignment::Singleton(node));
    // }

    pub fn overwrite_mapping(&mut self, node: NodeIndex, block: BlockAssignment) {
        if let BlockAssignment::Singleton(_) = &block {
            self.singleton_count += 1;
        }
        self.new_mapping.insert(node, block);
    }
}

pub struct KBisimulationOutcome {
    /// The current partitioning of the nodes
    pub blocks: Vec<Option<Block>>,
    /// A list of all blocks in blocks that are None in blocks
    pub freeblock_indices: Vec<BlockIndex>,
    /// A reverse index of blocks,for each node, it maps to the block in which the node is
    pub node_to_block: Node2Block,
    /// Blocks that might split in the next partition refinement iteration
    pub dirty_blocks: Vec<BlockIndex>,
    /// blocks that have outgoing data edges to blocks that got split, but are not marked dirty because we know they won't split
    /// (either by virtue of being singletons or because they are below the min support).
    pub semi_dirty_blocks: Vec<BlockAssignment>,
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
        self.node_to_block.singleton_count
    }
}
#[derive(Clone, Copy)]
pub struct GlobalBlockIndexAndLevel {
    pub global_id: GlobalBlockIndex,
    pub level: LevelIndex,
}

// // NB: we assume global ids are unique and therefore enough for hashing and equality
// impl Hash for GlobalBlockIndexAndLevel {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.global_id.hash(state);
//     }
// }
// impl PartialEq for GlobalBlockIndexAndLevel {
//     fn eq(&self, other: &Self) -> bool {
//         self.global_id == other.global_id
//     }
// }
// impl Eq for GlobalBlockIndexAndLevel {}

enum DataEdgeTarget {
    Refined(GlobalBlockIndexAndLevel),
    Invariant(GlobalBlockIndexAndLevel),
}

// #[derive(Eq, PartialEq)]
// struct IndexAndSignature<'a> {
//     // the current index into the signature
//     index: usize,
//     signature: &'a Vec<(EdgeType, BlockAssignment)>,
// }

// impl<'a> Ord for IndexAndSignature<'a> {
//     // The ordering is on the block of the current index
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.signature[self.index].cmp(&other.signature[other.index])
//     }
// }

// // Ord also requires PartialOrd
// impl<'a> PartialOrd for IndexAndSignature<'a> {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

#[derive(Clone)]
pub struct DataEdgeCounter {
    pub condensed_counts: Vec<usize>,
    pub uncondensed_counts: Vec<usize>,
}

impl DataEdgeCounter {
    fn new() -> Self {
        DataEdgeCounter {
            condensed_counts: vec![0],
            uncondensed_counts: vec![0],
        }
    }

    fn increment_by_interval(&mut self, start_level: u64, end_level: u64) {
        let mut uncondensed_offset = 1;
        for i in (start_level as usize)..self.condensed_counts.len() {
            self.condensed_counts[i] += 1;
            self.uncondensed_counts[i] += uncondensed_offset;
            if i < end_level as usize {
                uncondensed_offset += 1;
            }
        }
    }

    fn add_level(&mut self) {
        self.condensed_counts
            .push(*self.condensed_counts.last().unwrap());
        self.uncondensed_counts
            .push(*self.uncondensed_counts.last().unwrap());
    }
}

pub struct SharedBisimulationState<W1: Write, W2: Write> {
    pub i: LevelIndex,
    global_largest_block_id: BlockIndex,
    pub previous_block_mapping: FxHashMap<BlockIndex, GlobalBlockIndexAndLevel>,
    pub singleton_mapping: FxHashMap<NodeIndex, GlobalBlockIndexAndLevel>,
    // output_directory: PathBuf,
    refines_writer: W1,
    new_mappings: FxHashMap<NodeIndex, GlobalBlockIndexAndLevel>,
    to_be_removed_local_ids: FxHashSet<BlockIndex>,
    previous_refines_map: FxHashMap<BlockAssignment, GlobalBlockIndexAndLevel>,
    new_refines_map: FxHashMap<BlockAssignment, GlobalBlockIndexAndLevel>,
    data_edge_writer: W2,
    data_edge_counter: DataEdgeCounter,
}

impl<W1: Write, W2: Write> SharedBisimulationState<W1, W2> {
    fn new(
        bisimulation_outcome: &KBisimulationOutcome,
        // output_dir: impl AsRef<Path>,
        refines_writer: W1,
        data_edge_writer: W2,
    ) -> Result<Self> {
        let i = 1; // TODO NB the current code sets this to 0u64 initially

        // Track the living blocks
        let mut previous_block_mapping = FxHashMap::default();
        for new_id in 0..bisimulation_outcome.blocks.len() {
            previous_block_mapping.insert(
                new_id,
                GlobalBlockIndexAndLevel {
                    global_id: new_id,
                    level: 0,
                },
            );
        }

        // Track the singletons
        let mut global_largest_block_id = bisimulation_outcome.blocks.len() - 1;
        let mut singleton_mapping = FxHashMap::default();
        for block in &bisimulation_outcome.node_to_block.mapping {
            match block {
                BlockAssignment::Block(_) => continue,
                BlockAssignment::Singleton(node_idx) => {
                    global_largest_block_id += 1;
                    singleton_mapping.insert(
                        *node_idx,
                        GlobalBlockIndexAndLevel {
                            global_id: global_largest_block_id,
                            level: 0,
                        },
                    );
                }
            }
        }

        // let output_directory = output_dir.as_ref().to_path_buf();

        // let refines_dir = output_directory.join("refines");
        // fs::create_dir(refines_dir.clone())?;
        // let refines_path = refines_dir.join(format!("refines_{}", i));
        // let refines_file = File::create(refines_path)?;
        // let refines_writer = BufWriter::new(refines_file);

        // let data_edge_path = output_directory.join("data_edges");
        // let data_edge_file = File::create(data_edge_path)?;
        // let data_edge_writer = BufWriter::new(data_edge_file);

        let mut data_edge_counter = DataEdgeCounter::new();
        data_edge_counter.add_level();

        Ok(Self {
            i,
            global_largest_block_id,
            previous_block_mapping,
            singleton_mapping,
            // output_directory,
            refines_writer,
            new_mappings: FxHashMap::default(),
            to_be_removed_local_ids: FxHashSet::default(),
            previous_refines_map: FxHashMap::default(),
            new_refines_map: FxHashMap::default(),
            data_edge_writer,
            data_edge_counter,
        })
    }

    pub fn refines_writer_mut(&mut self) -> &mut W1 {
        &mut self.refines_writer
    }

    pub fn data_edge_writer_mut(&mut self) -> &mut W2 {
        &mut self.data_edge_writer
    }

    pub fn update_level(&mut self) -> Result<()> {
        for to_be_removed_local_id in self.to_be_removed_local_ids.drain() {
            self.previous_block_mapping.remove(&to_be_removed_local_id);
        }

        self.previous_block_mapping
            .extend(self.new_mappings.drain());

        self.i += 1;

        // let refine_path = self
        //     .output_directory
        //     .join("refines")
        //     .join(format!("refines_{}", self.i));
        // let file = File::create(refine_path)?;
        // self.refines_writer = BufWriter::new(file);

        self.data_edge_counter.add_level();

        Ok(())
    }

    fn refine_callback(
        &mut self,
        refine_source_block: &BlockAssignment,
        refine_target_block: &BlockAssignment,
    ) -> Result<()> {
        // TODO it should probably not be the refine callback's responsibility to handle global blocks
        match (refine_source_block, refine_target_block) {
            (BlockAssignment::Block(source_local), BlockAssignment::Block(target_local)) => {
                self.global_largest_block_id += 1;
                let source_global = self.global_largest_block_id;

                let GlobalBlockIndexAndLevel {
                    global_id: target_global,
                    level: target_level,
                } = self.previous_block_mapping.get(target_local).unwrap();

                //println!("{} -> {}", source_global, target_global);
                self.refines_writer
                    .write_all(&source_global.to_be_bytes())?;
                self.refines_writer
                    .write_all(&target_global.to_be_bytes())?;

                self.new_refines_map.insert(
                    (*refine_source_block).clone(),
                    GlobalBlockIndexAndLevel {
                        global_id: *target_global,
                        level: *target_level,
                    },
                ); // Store the current refines map in memory

                self.new_mappings.insert(
                    *source_local,
                    GlobalBlockIndexAndLevel {
                        global_id: source_global,
                        level: self.i,
                    },
                );
            }
            (BlockAssignment::Singleton(node_index), BlockAssignment::Block(target_local)) => {
                //let source_global = (-(*source_local as i64)) - 1;
                self.global_largest_block_id += 1;
                let source_global = self.global_largest_block_id;

                let GlobalBlockIndexAndLevel {
                    global_id: target_global,
                    level: target_level,
                } = self.previous_block_mapping.get(target_local).unwrap();

                // println!("s - {} -> {}", source_global, target_global);
                self.refines_writer
                    .write_all(&source_global.to_be_bytes())?;
                self.refines_writer
                    .write_all(&target_global.to_be_bytes())?;

                self.new_refines_map.insert(
                    (*refine_source_block).clone(),
                    GlobalBlockIndexAndLevel {
                        global_id: *target_global,
                        level: *target_level,
                    },
                ); // Store the current refines map in memory

                self.singleton_mapping.insert(
                    *node_index,
                    GlobalBlockIndexAndLevel {
                        global_id: source_global,
                        level: self.i,
                    },
                );
            }
            _ => {
                panic!("Not a valid edge");
            }
        };
        Ok(())
    }

    fn refine_target_can_be_freed(&mut self, local_target_id: &BlockIndex) -> Result<()> {
        self.to_be_removed_local_ids.insert(*local_target_id);
        Ok(())
    }

    // fn take_previous_refines_map(&mut self) -> HashMap<BlockAssignment, GlobalBlockIndexAndLevel, FxBuildHasher> {
    //     std::mem::take(&mut self.previous_refines_map)
    // }

    // This function is for the dirty blocks that did not split
    // Because these blocks did not split, we only have to check for the objects/targets of their outgoing data edges to is whether they changed and if so, (later) emit the respective data edges
    // This means we only have to consider data edges with blocks that are keys in self.previous_refines_map
    // As an argument it takes an iteator over all of the parts in the different signatures
    fn refined_signatures_to_unique_signature_parts<'a, I>(
        &mut self,
        sig_keys: I,
    ) -> Option<Vec<(u32, GlobalBlockIndex, LevelIndex)>>
    where
        I: IntoIterator<Item = &'a (EdgeType, BlockAssignment)>,
    {
        // Return early when the level is 0 or 1, because at those levels there is not enough information to emit any data edges
        if self.i <= 1 {
            return None;
        }

        let mut signature_pieces_union = Vec::new();

        for (edge_type, block) in sig_keys {
            let Some(GlobalBlockIndexAndLevel { global_id, level }) =
                self.previous_refines_map.get(block)
            else {
                continue;
            };
            signature_pieces_union.push((*edge_type, *global_id, *level));
        }

        signature_pieces_union.sort();
        signature_pieces_union.dedup();
        Some(signature_pieces_union)
    }

    fn get_global_id(&self, block: &BlockAssignment) -> &GlobalBlockIndexAndLevel {
        match block {
            BlockAssignment::Block(block_id) => self
                .previous_block_mapping
                .get(block_id)
                .expect("Block not found in previous_block_mapping"),
            BlockAssignment::Singleton(singleton_id) => self
                .singleton_mapping
                .get(singleton_id)
                .expect("Singleton not found in singleton_mapping"),
        }
    }

    // // This function is for finding all outgoing data edges for splitting blocks, so the can be emitted later
    fn signatures_to_unique_signature_parts<'a, I>(
        &mut self,
        sig_keys: I,
    ) -> Option<Vec<(u32, GlobalBlockIndex, LevelIndex)>>
    where
        I: IntoIterator<Item = &'a Vec<(EdgeType, BlockAssignment)>>,
    {
        // Return early when the level is 0 or 1, because at those levels there is not enough information to emit any data edges
        if self.i <= 1 {
            return None;
        }
        // The helper function that handles the mapping to global signatures, along with the starting levels
        let signature_to_global_mapper_helper = |block: &BlockAssignment| -> DataEdgeTarget {
            let get_previous_global_id_fallback = || {
                let target = *self.get_global_id(block);
                DataEdgeTarget::Invariant(target)
            };
            // return the  global_id_and_level
            self.previous_refines_map
                .get(block)
                .copied()
                .map_or_else(get_previous_global_id_fallback, DataEdgeTarget::Refined)
        };

        // Convenient way to map the target and pass on the edge type
        let map_piece = |piece: &(u32, BlockAssignment)| {
            let target = signature_to_global_mapper_helper(&piece.1);
            (piece.0, target)
        };

        let iters = sig_keys.into_iter().kmerge().dedup();

        let mut signature_pieces_union = Vec::new();

        let mut refined_targets = Vec::new();

        for possibly_new_piece in iters {
            let (pred, mapped_target) = map_piece(possibly_new_piece);
            match mapped_target {
                DataEdgeTarget::Refined(GlobalBlockIndexAndLevel { global_id, level }) => {
                    refined_targets.push((pred, global_id, level));
                }
                DataEdgeTarget::Invariant(GlobalBlockIndexAndLevel { global_id, level }) => {
                    signature_pieces_union.push((pred, global_id, level));
                }
            };
        }

        refined_targets.sort();
        refined_targets.dedup();
        signature_pieces_union.extend(refined_targets);
        Some(signature_pieces_union)
    }

    // DUMMY IMPLEMENTATION TO TEST THE CODE
    // This function is for finding all outgoing data edges for splitting blocks, so the can be emitted later
    // fn signatures_to_unique_signature_parts<'a, I>(
    //     &mut self,
    //     sig_keys: I,
    // ) -> Option<Vec<(u32, GlobalBlockIndex, LevelIndex)>>
    // where
    //     I: IntoIterator<Item = &'a Vec<(EdgeType, BlockAssignment)>>,
    // {
    //     // Return early when the level is 0 or 1, because at those levels there is not enough information to emit any data edges
    //     if self.i <= 1 {
    //         return None;
    //     }
    //     // The helper function that handles the mapping to global signatures, along with the starting levels
    //     let signature_to_global_mapper_helper = |block: &BlockAssignment| -> DataEdgeTarget {
    //         let get_previous_global_id_fallback = || {
    //             let target = self.get_global_id(block).clone();
    //             DataEdgeTarget::Invariant(target)
    //         };
    //         let global_id_and_level = self.previous_refines_map.get(block).copied().map_or_else(
    //             || get_previous_global_id_fallback(),
    //             |target| DataEdgeTarget::Refined(target),
    //         );
    //         global_id_and_level
    //     };

    //     // Convenient way to map the target and pass on the edge type
    //     let map_piece = |piece: &(u32, BlockAssignment)| {
    //         let target = signature_to_global_mapper_helper(&piece.1);
    //         (piece.0, target)
    //     };

    //     // DEBUG: This is just for debugging
    //     let mut taken_sigs = Vec::new();
    //     for sig in sig_keys.into_iter() {
    //         assert!(sig.is_sorted());
    //         // assert!(sig.is_sorted_by_key(|k| Reverse(k)));  // Check for reverse sorting
    //         // println!("{:?}", sig);
    //         taken_sigs.push(sig);
    //     }

    //     let mut mapped_signatures: Vec<_> = taken_sigs
    //         .into_iter()
    //         .flat_map(|v| {
    //             v.iter().map(|x| {
    //                 let (pred, target) = map_piece(x);
    //                 let (global_id, level) = match target {
    //                     DataEdgeTarget::Refined(id_level) | DataEdgeTarget::Invariant(id_level) => {
    //                         let GlobalBlockIndexAndLevel { global_id, level } = id_level;
    //                         (global_id, level)
    //                     }
    //                 };
    //                 (pred, global_id, level)
    //             })
    //         })
    //         .collect();
    //     mapped_signatures.sort();
    //     mapped_signatures.dedup();
    //     Some(mapped_signatures)
    // }

    pub fn data_edge_callback(
        &mut self,
        (subject, predicate, object): (GlobalBlockIndex, u32, GlobalBlockIndex),
        (start_level, end_level): (LevelIndex, LevelIndex),
    ) -> Result<()> {
        // println!("{} -{}-> {}", subject, predicate, object);
        let end_level_or_fixed_point = if end_level == 0 {
            self.i // NB: we use 0 as a stand-in for infinity. We assume self.i is currently equal to the fixed point
        } else {
            end_level
        };
        self.data_edge_counter
            .increment_by_interval(start_level, end_level_or_fixed_point);
        self.data_edge_writer.write_all(&subject.to_be_bytes())?;
        self.data_edge_writer.write_all(&predicate.to_be_bytes())?;
        self.data_edge_writer.write_all(&object.to_be_bytes())?;
        self.data_edge_writer
            .write_all(&start_level.to_be_bytes())?;
        self.data_edge_writer.write_all(&end_level.to_be_bytes())?;
        Ok(())
    }

    pub fn refines_edge_count(&self) -> usize {
        self.new_refines_map.len()
    }

    pub fn data_edge_counter(&self) -> DataEdgeCounter {
        self.data_edge_counter.clone()
    }
}

pub struct FullBisimulationState<W1: Write, W2: Write> {
    pub shared_state: SharedBisimulationState<W1, W2>,
    pub current_outcome: KBisimulationOutcome,
}

impl<W1: Write, W2: Write> FullBisimulationState<W1, W2> {
    pub fn new(
        bisimulation_outcome: KBisimulationOutcome,
        // output_dir: impl AsRef<Path>,
        refines_writer: W1,
        data_edge_writer: W2,
    ) -> Result<Self> {
        Ok(Self {
            shared_state: SharedBisimulationState::new(&bisimulation_outcome, refines_writer, data_edge_writer)?,
            current_outcome: bisimulation_outcome,
        })
    }

    fn steal_outcome(self) -> (PartialBisimulationState<W1, W2>, KBisimulationOutcome) {
        let FullBisimulationState {
            mut shared_state,
            current_outcome,
        } = self;
        shared_state.previous_refines_map = std::mem::take(&mut shared_state.new_refines_map);
        (PartialBisimulationState { shared_state }, current_outcome)
    }

    pub fn into_parts(self) -> (SharedBisimulationState<W1, W2>, KBisimulationOutcome) {
        let FullBisimulationState {
            shared_state,
            current_outcome,
        } = self;
        (shared_state, current_outcome)
    }
}

struct PartialBisimulationState<W1: Write, W2: Write> {
    shared_state: SharedBisimulationState<W1, W2>,
}

impl<W1: Write, W2: Write> PartialBisimulationState<W1, W2> {
    fn restore_outcome(self, new_outcome: KBisimulationOutcome) -> FullBisimulationState<W1, W2> {
        FullBisimulationState {
            shared_state: self.shared_state,
            current_outcome: new_outcome,
        }
    }
}

pub fn get_i_bisimulation<W1: Write, W2: Write>(
    graph: &FlatGraph,
    predecessors: &Predecessors, // the predecessors computed with graph.build_predecessors()
    // We take ownership of the previous outcome and will reuse parts of this for the current outcome
    bisimulation_state: FullBisimulationState<W1, W2>,
    min_support: usize,
) -> Result<FullBisimulationState<W1, W2>> {
    let (mut partial_bisimulation_state, prev_outcome) = bisimulation_state.steal_outcome();
    // We take the parts out of the previous_outcome for reuse
    let mut k_blocks = prev_outcome.blocks;

    let mut dirty_blocks = prev_outcome.dirty_blocks;

    let mut this_level_mapper =
        InternalNode2BlockMapper::new_from_previous(prev_outcome.node_to_block);

    let mut freeblock_indices = prev_outcome.freeblock_indices;

    let mut refined_block_set: Vec<Block> = Vec::new();

    let mut semi_dirty_blocks = prev_outcome.semi_dirty_blocks;
    {
        // This joint_signature is reused many times in the next for loop.
        let mut joint_signature = FxHashSet::default();

        for semi_dirty_idx in semi_dirty_blocks.drain(..) {
            // let block_nodes = match semi_dirty_idx {
            //     BlockAssignment::Block(block_id) => std::borrow::Cow::Borrowed(&k_blocks[block_id].as_ref().unwrap().nodes),  // We are sure this block must exist, so we can unwrap and borrow
            //     BlockAssignment::Singleton(node_id) => std::borrow::Cow::Owned(vec![node_id])  // Create new singleton block to own
            // };

            joint_signature.clear();

            let block_nodes: std::borrow::Cow<'_, [usize]> = match semi_dirty_idx {
                BlockAssignment::Block(_) => {
                    if min_support > 1 {
                        todo!(
                            "Incomplete implementation for non-singletons below the min_support."
                        );
                        // We are sure this block must exist, so we can unwrap and borrow
                    }
                    continue;
                } // TODO
                BlockAssignment::Singleton(node_id) => std::borrow::Cow::Owned(vec![node_id]), // Create new singleton block to own
            };

            for &v in block_nodes.iter() {
                graph.get_node(v).edges.iter().for_each(|e| {
                    joint_signature.insert((
                        e.label,
                        this_level_mapper.get_previous_level_block_idx(e.target),
                    ));
                });
            }

            // Because we already know the block doesn't split, we use `refined_signatures_to_unique_signature_parts()`
            let targets = partial_bisimulation_state
                .shared_state
                .refined_signatures_to_unique_signature_parts(joint_signature.iter())
                .unwrap_or_default();
            let GlobalBlockIndexAndLevel {
                global_id: global_subject,
                level: subject_level,
            } = *partial_bisimulation_state
                .shared_state
                .get_global_id(&semi_dirty_idx);
            for (edge_type, global_target, target_level) in targets {
                let start_level = std::cmp::max(subject_level, target_level + 1);
                let end_level = partial_bisimulation_state.shared_state.i - 1;
                // println!("DEBUG s-inc: ({}, {}, {}) [{}, {}]", global_subject, edge_type, global_target, start_time, end_time);
                partial_bisimulation_state.shared_state.data_edge_callback(
                    (global_subject, edge_type, global_target),
                    (start_level, end_level),
                )?;
            }
        }
    }
    // Iterate through dirty blocks from the previous step
    for dirty_idx in dirty_blocks.drain(..) {
        // we are sure this block must exist, so we can unwrap
        let block_ref = k_blocks[dirty_idx].as_ref().unwrap();

        // We don't even mark blocks below the min_support as dirty, so they must not exist
        debug_assert!(block_ref.nodes.len() > min_support);

        // signature_t: Map of (EdgeLabel, TargetBlockID) -> Nodes
        let mut signatures: FxHashMap<Vec<(EdgeType, BlockAssignment)>, Vec<NodeIndex>> =
            FxHashMap::default();

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
            let sig: Vec<(EdgeType, BlockAssignment)> = btsig.into_iter().collect();

            signatures.entry(sig).or_default().push(v);
        }

        // let mut target_candidates: Vec<(u32, u64)> = signatures_to_unique_signature_parts(&signatures).into_iter().map(f);

        if signatures.len() <= 1 {
            // Check for any data edges that need to be persisted
            // TODO clean this up (perhaps move the map through previous_refines_map to the signatures_to_unique_signature_parts function itself)
            let targets = partial_bisimulation_state
                .shared_state
                .refined_signatures_to_unique_signature_parts(signatures.keys().flatten())
                .unwrap_or_default();
            let GlobalBlockIndexAndLevel {
                global_id: global_subject,
                level: subject_level,
            } = *partial_bisimulation_state
                .shared_state
                .get_global_id(&BlockAssignment::Block(dirty_idx));
            for (edge_type, global_target, target_level) in targets {
                let start_level = std::cmp::max(subject_level, target_level + 1);
                let end_level = partial_bisimulation_state.shared_state.i - 1;
                // println!("DEBUG f-inc: ({}, {}, {}) [{}, {}]", global_subject, edge_type, global_target, start_time, end_time);
                partial_bisimulation_state.shared_state.data_edge_callback(
                    (global_subject, edge_type, global_target),
                    (start_level, end_level),
                )?;
            }

            continue;
        } // No split occurred

        // Persist outgoing data edges
        // TODO clean this up (perhaps move the map through previous_refines_map to the signatures_to_unique_signature_parts function itself)
        // TODO add a function to get the global id and
        let targets = partial_bisimulation_state
            .shared_state
            .signatures_to_unique_signature_parts(signatures.keys())
            .unwrap_or_default();
        let GlobalBlockIndexAndLevel {
            global_id: global_subject,
            level: subject_level,
        } = *partial_bisimulation_state
            .shared_state
            .get_global_id(&BlockAssignment::Block(dirty_idx));
        for (edge_type, global_target, target_level) in targets.into_iter() {
            let start_level = std::cmp::max(subject_level, target_level + 1);
            let end_level = partial_bisimulation_state.shared_state.i - 1;
            // println!("DEBUG f-out: ({}, {}, {}) [{}, {}]", global_subject, edge_type, global_target, start_time, end_time);
            partial_bisimulation_state.shared_state.data_edge_callback(
                (global_subject, edge_type, global_target),
                (start_level, end_level),
            )?;
        }

        // We take ownership of the block and put a None at that spot in k_block, and mark that block as free

        let block = k_blocks[dirty_idx].take().unwrap();
        freeblock_indices.push(dirty_idx);

        let refines_object: BlockAssignment = BlockAssignment::Block(dirty_idx);

        let mut only_singletons = true;

        for (_, nodes) in signatures.into_iter() {
            if nodes.len() == 1 {
                let refines_subject: BlockAssignment = BlockAssignment::Singleton(nodes[0]);
                partial_bisimulation_state
                    .shared_state
                    .refine_callback(&refines_subject, &refines_object)?;
                // this_level_mapper.put_into_singleton(global_id);
                this_level_mapper.overwrite_mapping(nodes[0], refines_subject);
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
                    this_level_mapper.overwrite_mapping(node, refines_subject.clone());
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
    // dirty_blocks.clear();  // TODO removed this because the above loop can just drain

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

                        let block_idx = match dirty_block_id {
                            BlockAssignment::Singleton(_) => {
                                // If it is a singleton, it can never split, so no need to mark dirty
                                semi_dirty_blocks.push(dirty_block_id);
                                continue;
                            }
                            BlockAssignment::Block(block_idx) => block_idx,
                        };

                        // let block_idx = dirty_block_id;

                        // Only mark if the block size meets the min_support requirement
                        if k_blocks[block_idx].as_ref().unwrap().nodes.len() >= min_support {
                            // only add it if it is not a duplicate, this is a heuristic saving by checking whether it is the same as the previous
                            if let Some(last) = dirty_blocks.last()
                                && *last == block_idx
                            {
                                continue;
                            }
                            dirty_blocks.push(block_idx);
                        } else {
                            if let Some(last) = semi_dirty_blocks.last()
                                && *last == dirty_block_id
                            {
                                continue;
                            }

                            semi_dirty_blocks.push(dirty_block_id);
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

    semi_dirty_blocks.sort();
    semi_dirty_blocks.dedup();
    // it is likely that each next level semi-dirty block vector has fewer elements, hence shrinking
    semi_dirty_blocks.shrink_to_fit();

    let full_bisimulation_state =
        partial_bisimulation_state.restore_outcome(KBisimulationOutcome {
            blocks: k_blocks,
            dirty_blocks,
            semi_dirty_blocks,
            node_to_block: this_level_mapper.commit_new_mapping(),
            freeblock_indices,
        });

    Ok(full_bisimulation_state)
}

pub fn get_typed_0_bisimulation(graph: &FlatGraph, rdf_type_id: EdgeType) -> KBisimulationOutcome {
    let mut partition_map: FxHashMap<Vec<NodeIndex>, Vec<NodeIndex>> = FxHashMap::default();

    //HashMap::new();

    for node_idx in 0..graph.get_size() {
        let node = graph.get_node(node_idx);
        let mut type_set: BTreeSet<usize> = BTreeSet::new();
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
        let block_idx = new_blocks.len();
        if nodes.len() == 1 {
            mapper.overwrite_mapping(nodes[0], BlockAssignment::Singleton(nodes[0]));
        } else {
            for &node_idx in &nodes {
                mapper.overwrite_mapping(node_idx, BlockAssignment::Block(block_idx));
            }
            new_blocks.push(Some(Block { nodes, f: 0 }));
            dirty.push(block_idx);
        }
    }

    KBisimulationOutcome {
        blocks: new_blocks,
        dirty_blocks: dirty,
        semi_dirty_blocks: Vec::new(), // We don't use semi-dirty blocks at i < 2, so it is safe to mark as empty for now
        node_to_block: mapper.commit_new_mapping(),
        freeblock_indices: Vec::new(),
    }
}

pub fn get_0_bisimulation(graph: &FlatGraph) -> KBisimulationOutcome {
    let node_count = graph.get_size();

    // Create the initial block containing all node indices
    // C++: block->reserve(amount); for (unsigned int i = 0; i < amount; i++) { block->emplace_back(i); }
    let initial_block: Vec<usize> = (0..node_count).collect();

    let blocks = vec![Some(Block {
        nodes: initial_block,
        f: 0,
    })];

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
        semi_dirty_blocks: Vec::new(), // We don't use semi-dirty blocks at i < 2, so it is safe to mark as empty for now
        node_to_block: mapper,
        freeblock_indices: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;
    use std::fs::File;
    use std::io::BufWriter;

    // --- Mock Setup Helper ---
    fn setup_mock_state(i: LevelIndex) -> SharedBisimulationState<BufWriter<File>, BufWriter<File>> {
        let mut previous_refines_map = FxHashMap::default();
        let mut previous_block_mapping = FxHashMap::default();

        // Seed the map for the "Refined" path test.
        // If the block is 10, it maps to global ID 100, level 5.
        previous_refines_map.insert(
            BlockAssignment::Block(10),
            GlobalBlockIndexAndLevel {
                global_id: 100,
                level: 5,
            },
        );

        // Seed the map for the "Invariant Fallback" path test.
        // If the block is 999, it is NOT in previous_refines_map,
        // but get_global_id() will find it here.
        previous_block_mapping.insert(
            999,
            GlobalBlockIndexAndLevel {
                global_id: 200,
                level: 3,
            },
        );

        // // Set a dummy output directory
        // let dummy_output = "dummy_output";

        // We use tempfile() so the files are created and destroyed cleanly in the OS temp directory
        let temp_file_1 = tempfile().expect("Failed to create temp file for refines_writer");
        let temp_file_2 = tempfile().expect("Failed to create temp file for data_edge_writer");

        SharedBisimulationState {
            i,
            global_largest_block_id: 0,
            previous_block_mapping,
            singleton_mapping: FxHashMap::default(),
            // output_directory: dummy_output.into(),
            refines_writer: BufWriter::new(temp_file_1),
            new_mappings: FxHashMap::default(),
            to_be_removed_local_ids: FxHashSet::default(),
            previous_refines_map,
            new_refines_map: FxHashMap::default(),
            data_edge_writer: BufWriter::new(temp_file_2),
            data_edge_counter: DataEdgeCounter::new(),
        }
    }

    #[test]
    fn test_early_return_when_i_is_0_or_1() {
        let mut state_0 = setup_mock_state(0);
        let mut state_1 = setup_mock_state(1);

        let signatures: Vec<Vec<(EdgeType, BlockAssignment)>> = vec![];

        assert_eq!(
            state_0.signatures_to_unique_signature_parts(&signatures),
            None
        );
        assert_eq!(
            state_1.signatures_to_unique_signature_parts(&signatures),
            None
        );
    }

    #[test]
    fn test_refined_mapping_from_previous_refines_map() {
        let mut state = setup_mock_state(2);

        // Input MUST be sorted to pass the internal assert!(sig.is_sorted())
        let sig1 = vec![(1, BlockAssignment::Block(10))];
        let signatures = vec![sig1];

        let result = state
            .signatures_to_unique_signature_parts(&signatures)
            .unwrap();

        // Expect it to use `previous_refines_map` (global_id: 100, level: 5)
        assert_eq!(result, vec![(1, 100, 5)]);
    }

    #[test]
    fn test_invariant_fallback_mapping() {
        let mut state = setup_mock_state(2);

        // Block 999 is NOT in `previous_refines_map`, triggering the invariant fallback.
        // It should pull from `previous_block_mapping` (global_id: 200, level: 3).
        let sig1 = vec![(2, BlockAssignment::Block(999))];
        let signatures = vec![sig1];

        let result = state
            .signatures_to_unique_signature_parts(&signatures)
            .unwrap();

        assert_eq!(result, vec![(2, 200, 3)]);
    }

    #[test]
    fn test_flattening_sorting_and_deduplication() {
        let mut state = setup_mock_state(2);

        // Vector 1 (Sorted)
        let sig1 = vec![
            (1, BlockAssignment::Block(10)), // Maps to (1, 100, 5)
            (3, BlockAssignment::Block(10)), // Maps to (3, 100, 5)
        ];

        // Vector 2 (Sorted) - Contains a duplicate to test deduplication
        let sig2 = vec![
            (1, BlockAssignment::Block(10)), // Duplicate, should be deduplicated
            (2, BlockAssignment::Block(10)), // Maps to (2, 100, 5)
        ];

        let signatures = vec![sig1, sig2];
        let result = state
            .signatures_to_unique_signature_parts(&signatures)
            .unwrap();
        print!("{:?}", result);
        assert_eq!(result, vec![(1, 100, 5), (2, 100, 5), (3, 100, 5),]);
    }

    #[test]
    fn test_empty_input_iterator() {
        let mut state = setup_mock_state(2);
        let signatures: Vec<Vec<(EdgeType, BlockAssignment)>> = vec![];

        let result = state
            .signatures_to_unique_signature_parts(&signatures)
            .unwrap();

        assert_eq!(result, vec![]);
    }
}
