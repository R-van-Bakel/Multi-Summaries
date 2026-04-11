use biterator::{Bit, Biterator};
use core::panic;
use fxhash::FxHashMap;
use itertools::Itertools;
use memmap2::MmapOptions;
use rayon::prelude::*;
use std::cmp::max;
use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::iter::repeat;
use std::path::Path;
use std::sync::mpsc::{self, channel};
use std::thread;
use thiserror::Error;

use crate::msd_radix_sort;

// In Rust, 'usize' is the standard type for indexing arrays/vectors.
// On 64-bit systems, this matches the 64-bit requirement.
pub type EdgeType = u32;
pub type NodeIndex = usize;

pub const BYTES_PER_ENTITY: usize = 5;
pub const BYTES_PER_PREDICATE: usize = 4;
pub const BYTES_PER_TRIPLE: usize = 2 * BYTES_PER_ENTITY + BYTES_PER_PREDICATE;

#[derive(Debug, Error)]
pub enum GraphValidationError {
    #[error("I/O error during graph validation: {0}")]
    Io(#[from] std::io::Error),

    #[error(
        "The graph binary size ({num_bytes} bytes) is not divisible by the number of bytes per triple ({BYTES_PER_TRIPLE})"
    )]
    NotDivisible { num_bytes: usize },
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub label: EdgeType,
    pub target: NodeIndex,
}

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub edges: Vec<Edge>,
}

impl Node {
    pub fn new() -> Self {
        Self { edges: Vec::new() }
    }

    pub fn add_edge(&mut self, label: EdgeType, target: NodeIndex) {
        self.edges.push(Edge { label, target });
    }

    /// Adds the edge only if it is not a duplicate.
    /// Returns true if the edge was added.
    pub fn add_edge_if_not_duplicate(&mut self, label: EdgeType, target: NodeIndex) -> bool {
        // Iterate to check existence (O(N) for N edges in this node)
        for edge in &self.edges {
            if edge.label == label && edge.target == target {
                return false;
            }
        }
        self.edges.push(Edge { label, target });
        true
    }
}

#[derive(Debug, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
}

const TRIPLE_PER_CHUNK: usize = 1024 << 17;

pub struct RelStat {
    pub triple_count: u64,
    pub lowest_subject: NodeIndex,
    pub highest_subject: NodeIndex,
    pub lowest_object: NodeIndex,
    pub highest_object: NodeIndex,
}

impl Debug for RelStat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "([{}-{}] {} [{}-{}])",
            self.lowest_subject,
            self.highest_subject,
            self.triple_count,
            self.lowest_object,
            self.highest_object,
        )?;

        Ok(())
    }
}

impl Graph {
    pub fn new(expected_size: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(expected_size),
        }
    }

    pub fn add_vertex(&mut self) -> NodeIndex {
        let index = self.nodes.len();
        self.nodes.push(Node::new());
        index
    }

    pub fn get_size(&self) -> usize {
        self.nodes.len()
    }

    // Checks whether all targets of edges are pointing at an existing node
    // Checks whether all edge types are less than the max index
    pub fn check_consistency(&self) -> Result<(), String> {
        for node in self.nodes.iter() {
            for edge in node.edges.iter() {
                if edge.target >= self.nodes.len() {
                    return Err(format!(
                        "Target {} too large for graph with {} nodes",
                        edge.target,
                        self.nodes.len()
                    ));
                }
            }
        }
        Ok(())
    }

    /// Returns the total number of edges in the graph.
    /// (Equivalent to GetVertexCount in the Go implementation)
    pub fn get_total_edge_count(&self) -> usize {
        self.nodes.iter().map(|n| n.edges.len()).sum()
    }

    pub fn get_degree_histogram(&self) -> FxHashMap<usize, u64> {
        let mut counts: FxHashMap<usize, u64> = FxHashMap::default();
        for node in self.nodes.iter() {
            counts
                .entry(node.edges.len())
                .and_modify(|e| *e += 1)
                .or_insert(1);
        }
        counts
    }

    // returns for each relation,
    // 1. the number of triples using that relations,
    // 2. the lowest and highest entity in its subject position and
    // 3. the lowest and highest entity in its object position and
    pub fn get_relation_distribution(&self) -> FxHashMap<EdgeType, RelStat> {
        let mut counts: FxHashMap<EdgeType, RelStat> = FxHashMap::default();
        let node_enumeration = self.nodes.iter().enumerate();
        let edge_enumeration =
            node_enumeration.flat_map(|(index, n)| repeat(index).zip(n.edges.iter()));
        edge_enumeration.for_each(|(subject, edge)| {
            counts
                .entry(edge.label)
                .and_modify(|e| {
                    e.triple_count += 1;
                    e.lowest_subject = min(e.lowest_subject, subject);
                    e.highest_subject = max(e.highest_subject, subject);
                    e.lowest_object = min(e.lowest_object, edge.target);
                    e.highest_object = max(e.highest_object, edge.target);
                })
                .or_insert(RelStat {
                    triple_count: 1,
                    lowest_subject: subject,
                    highest_subject: subject,
                    lowest_object: edge.target,
                    highest_object: edge.target,
                });
        });
        counts
    }

    /// Resizes the graph to ensure it has at least `n` nodes.
    /// New nodes are initialized as empty.
    pub fn resize(&mut self, n: usize) {
        // We have to check because resize with will truncate if n < len.
        if n > self.nodes.len() {
            // resize_with automatically handles allocation and filling
            self.nodes.resize_with(n, Node::new);
        }
    }

    /// Reads a graph from a binary file.
    pub fn read_graph_from_file(&mut self, file_name: &str, reverse_edges: bool) -> io::Result<()> {
        let path = Path::new(file_name);
        let file = File::open(path)?;
        // 128KB buffer (Go code used ~129KB: 8 * 16184)
        let reader = BufReader::with_capacity(128 * 1024, file);
        self.read_graph_from_reader(reader, reverse_edges)
    }

    /// Reads a graph from reader.
    pub fn read_graph_from_reader<R: Read>(
        &mut self,
        reader: R,
        reverse_edges: bool,
    ) -> io::Result<()> {
        // We define the callback here to mutate 'self'
        read_triples_from_stream(reader, reverse_edges, |s, p, o| {
            let largest = max(s, o);
            // Ensure graph is large enough
            if largest >= self.nodes.len() {
                self.resize(largest + 1);
            }
            // Add the edge
            self.nodes[s].add_edge(p, o);
        })
    }

    #[inline(always)]
    fn process_variable_triples_per_chunk(
        buf: &[u8],
        triples: usize,
        reverse_edges: bool,
    ) -> (Vec<(NodeIndex, EdgeType, NodeIndex)>, NodeIndex) {
        debug_assert!(
            triples * (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE) == buf.len(),
            "{triples} , {}, {}",
            buf.len(),
            triples * (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE)
        );
        let mut result = vec![(0_usize, 0_u32, 0_usize); triples];

        const TRIPLE_LENGTH: usize = BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE;

        let mut max_entity: NodeIndex = 0;

        let mut start_index = 0;
        for output_triple in result.iter_mut() {
            //        for i in 0..triples {

            let mut subject: NodeIndex = 0;
            for b in 0..BYTES_PER_ENTITY {
                subject |= (buf[start_index + b] as NodeIndex) << (8 * b);
            }

            max_entity = max(subject, max_entity);

            let mut predicate: EdgeType = 0;
            for b in 0..BYTES_PER_PREDICATE {
                predicate |= (buf[start_index + BYTES_PER_ENTITY + b] as EdgeType) << (8 * b);
            }

            let mut object: NodeIndex = 0;
            for b in 0..BYTES_PER_ENTITY {
                object |= (buf[start_index + BYTES_PER_ENTITY + BYTES_PER_PREDICATE + b]
                    as NodeIndex)
                    << (8 * b);
            }
            max_entity = max(object, max_entity);

            if reverse_edges {
                *output_triple = (object, predicate, subject);
            } else {
                *output_triple = (subject, predicate, object);
            }
            start_index += TRIPLE_LENGTH;
        }
        (result, max_entity)
    }

    /// Reads a graph from a binary file.
    pub fn read_graph_parallel_memmmap(
        &mut self,
        file_name: impl AsRef<Path>,
        reverse_edges: bool,
    ) -> io::Result<()> {
        let path = file_name.as_ref();
        let file = File::open(path)?;

        let chunck_size = (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE) * TRIPLE_PER_CHUNK;

        let mmap = unsafe { MmapOptions::new().populate().map(&file) }?;
        let (sender, receiver) = channel();

        rayon::spawn(move || {
            mmap.chunks(chunck_size)
                .par_bridge()
                .map(|file_chunk| {
                    Self::process_variable_triples_per_chunk(
                        file_chunk,
                        file_chunk.len() / (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE),
                        reverse_edges,
                    )
                    // }
                })
                // Note: tried to do the following fold here in an attempt to speed things up.
                // This only made things go much slower.
                // .fold(
                //     || {
                //         // Initializer for local accumulator, we try to group by subject,
                //         // but only those that are adjacent, like posix uniq
                //         let identity: Vec<(NodeIndex, Vec<(EdgeType, NodeIndex)>)> = Vec::new();
                //         let max: NodeIndex = 0;
                //         (identity, max)
                //     },
                //     |(mut acc, acc_max), (triples, triples_max)| {
                //         triples
                //             .into_iter()
                //             .for_each(|(s, p, o)| match acc.last_mut() {
                //                 Some((s2, pos)) => {
                //                     if *s2 == s {
                //                         pos.push((p, o));
                //                     } else {
                //                         acc.push((s, vec![(p, o)]));
                //                     }
                //                 }
                //                 None => {
                //                     acc.push((s, vec![(p, o)]));
                //                 }
                //             });
                //         (
                //             acc,
                //             if acc_max > triples_max {
                //                 acc_max
                //             } else {
                //                 triples_max
                //             },
                //         )
                //     },
                // )
                // for_each_with allows us to clone the 'sender' for every thread safely
                // The ownership of sender is passed on and all its clones are only in scope inside the lambda.
                // Therefore, they will all be out of scope after the following statement and hence the channel will be closed.
                .for_each_with(sender, |s, x| s.send(x).unwrap());
        });

        let mut line_counter: u64 = 0;
        for (triple_block, max_entity) in receiver {
            if max_entity >= self.nodes.len() {
                self.resize(max_entity + 1);
            }

            for same_subject in triple_block.chunk_by(|(s1, _, _), (s2, _, _)| s1 == s2) {
                let subject = same_subject[0].0;
                let edges = &mut self.nodes[subject].edges;

                let count_with_same_subject = same_subject.len();

                edges.reserve(count_with_same_subject);

                for (_, p, o) in same_subject {
                    // Add the edge
                    edges.push(Edge {
                        label: *p,
                        target: *o,
                    });
                }
                // this counter is out of the tight loop for performace reasons
                let old_line_counter = line_counter;
                line_counter += count_with_same_subject as u64;
                if line_counter / 100_000_000 > old_line_counter / 100_000_000 {
                    println!("Read {:10} triples", line_counter);
                }
            }
        }
        println!("Read {:10} triples", line_counter);
        Ok(())
    }

    /// Reads a graph from a binary file.
    pub fn read_graph_memmmap(&mut self, file_name: &str, reverse_edges: bool) -> io::Result<()> {
        let path = Path::new(file_name);
        let file = File::open(path)?;

        let chunk_size = (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE) * TRIPLE_PER_CHUNK;

        let mmap = unsafe { MmapOptions::new().populate().map(&file) }?;

        mmap.chunks(chunk_size)
            .map(|file_chunk| {
                // println!("{}", file_chunk.len());

                Self::process_variable_triples_per_chunk(
                    file_chunk,
                    file_chunk.len() / (BYTES_PER_ENTITY * 2 + BYTES_PER_PREDICATE),
                    reverse_edges,
                )
            })
            .for_each(|(triple_block, max_entity)| {
                if max_entity >= self.nodes.len() {
                    self.resize(max_entity + 1);
                }

                for same_subject in triple_block.chunk_by(|(s1, _, _), (s2, _, _)| s1 == s2) {
                    let subject = same_subject[0].0;
                    let edges = &mut self.nodes[subject].edges;
                    edges.reserve(same_subject.len());

                    for (_, p, o) in same_subject {
                        // Add the edge
                        edges.push(Edge {
                            label: *p,
                            target: *o,
                        });
                    }
                }
            });

        Ok(())
    }

    // This version is implemented with a simple pipeline.
    // The one with memory mapped files showed to be much faster on the cluster.
    pub fn read_graph_pipelined(&mut self, file_name: &str, reverse_edges: bool) -> io::Result<()> {
        let path = Path::new(file_name);

        // provide backpressure so the reader doesn't exhaust RAM if the builder is slower.
        let (tx, rx) = mpsc::sync_channel(10_000);
        let file = File::open(path)?;

        // Spawn the Reader Thread
        let handle = thread::spawn(move || {
            let reader = BufReader::with_capacity(512 * 1024, file); // Large buffer

            // We reuse the sync helper from graph.rs, but we modify the callback
            // to send data down the channel instead of modifying a graph.
            let mut buffer: Vec<(usize, u32, usize)> = Vec::with_capacity(10_000_000);
            super::graph::read_triples_from_stream(reader, reverse_edges, |s, p, o| {
                // If receiver hangs up (error), we panic
                if buffer.len() < 10_000_000 {
                    buffer.push((s, p, o));
                } else {
                    tx.send(buffer.clone())
                        .expect("sending triples to the builder failed.");
                    buffer.clear();
                }
            })
            .expect("Reading from stream failed");
            //clean up final triples
            //clean up final triples
            tx.send(buffer.clone())
                .expect("sending triples to the builder failed.")
        });

        // Main Thread: Graph Builder
        // This runs without any locks because it is the *only* thing touching the graph.
        for vec_of_spo in rx {
            for (s, p, o) in vec_of_spo {
                let largest = max(s, o);
                if largest >= self.nodes.len() {
                    self.resize(largest + 1);
                }
                // Add the edge
                self.nodes[s].add_edge(p, o);
            }
        }
        // Wait for reader to finish checks for IO errors
        match handle.join() {
            Err(_) => Err(io::Error::other("Reader thread panicked")),
            Ok(_) => Ok(()),
        }
    }

    // This method is used to add extra features on the nodes of this graph.
    // It expects as its input a file with number_of_features*targets.len() bits.
    // We create (number_of_features * 2) edge types, ranging from feature_start_index to feature_start_index+(2*number_of_features)
    // The first number_of_features edge types are for when the bits are set to 0, the second half for when they are set to 1

    // So, for each node in nodes, we read number_of_features bits, convert these to the corresponding types
    // Then, for each type t, we add a triple (node, t, dummy) to the graph
    pub fn add_binary_features<R: Read>(
        &mut self,
        r: &mut R,
        number_of_features: EdgeType,
        feature_start_index: EdgeType,
        nodes: impl IntoIterator<Item = NodeIndex>,
        dummy_target: NodeIndex,
    ) -> io::Result<()> {
        let byte_iter = BufReader::new(r)
            .bytes()
            .map(|maybe_byte| maybe_byte.unwrap());

        let b = Biterator::new(byte_iter);
        let chunks = b.chunks(number_of_features as usize);
        let mut chunk_iter = chunks.into_iter();
        for (feature_bits, subject) in chunk_iter.by_ref().zip(nodes) {
            for (feature_index, bit) in feature_bits.enumerate() {
                let choice = match bit {
                    Bit::Zero => 0 as EdgeType,
                    Bit::One => 1 as EdgeType,
                };
                let label = feature_start_index + (2 * feature_index as EdgeType) + choice;
                self.nodes[subject].add_edge(label, dummy_target);
            }
        }

        // there could still be some bits left in the chunks iterator. Sanity check:
        let mut leftover_chunks: Vec<_> = chunk_iter.collect();
        match leftover_chunks.len() {
            0 => {}
            1 => {
                // We are sure there is 1 thing in leftover_chunk
                let leftover_bits = leftover_chunks.pop().unwrap();
                // there could be some bits left in the final byte, but nothing more
                let collection: Vec<Bit> = leftover_bits.collect();
                assert!(
                    collection.len() < 8,
                    "There are more than 7 non-processed bits left in the file. This must never happen. Aborting to prevent illegal state."
                )
            }
            _ => {
                panic!(
                    "There are multiple chunks left after zipping with the nodes, this must never happen. Aborting to prevent illegal state."
                )
            }
        };

        Ok(())
    }

    /// Writes the graph in the custom binary format.
    pub fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        for (subject_index, node) in self.nodes.iter().enumerate() {
            for edge in &node.edges {
                write_triple(w, subject_index, edge.label, edge.target)?;
            }
        }
        w.flush()?;
        Ok(())
    }

    pub fn build_predecessors(&self) -> Predecessors {
        let mut preds: Vec<Option<Vec<usize>>> = vec![None; self.nodes.len()];
        for (source_idx, node) in self.nodes.iter().enumerate() {
            for edge in &node.edges {
                // Ensure we don't add duplicate predecessors if your data has them
                match preds[edge.target].as_mut() {
                    None => {
                        preds[edge.target] = Some(vec![source_idx]);
                    }
                    Some(list) => {
                        list.push(source_idx);
                    }
                }
            }
        }
        // Sort and dedup each predecessor list
        // flatten to skip None's.
        for p in preds.iter_mut().flatten() {
            p.sort_unstable();
            p.dedup();
        }

        preds
    }
}

pub type Predecessors = Vec<Option<Vec<usize>>>;

// A graph where all edges are in a large array, indexed with the numbers in the node array.
// The edges for node i are edges[nodes[i-1]..nodes[i]]
pub struct FlatGraph {
    nodes: Vec<usize>,
    edges: Vec<Edge>,
}

impl FlatGraph {
    pub fn new(g: Graph) -> Self {
        // first we get the sizes to be allocated
        let node_count = g.nodes.len();
        let edge_count = g.get_total_edge_count();
        let mut sg = FlatGraph {
            nodes: Vec::with_capacity(node_count),
            edges: Vec::with_capacity(edge_count),
        };

        let mut nodes: VecDeque<Node> = VecDeque::from(g.nodes);

        while !nodes.is_empty() {
            let node: Node = nodes.pop_front().unwrap();
            let edges_in_this_node_count = node.edges.len();
            sg.nodes.push(sg.edges.len() + edges_in_this_node_count);
            // we move all edges out of the node
            sg.edges.extend(node.edges.into_iter());
        }

        sg
    }

    pub fn get_size(&self) -> usize {
        self.nodes.len()
    }

    pub fn get_node(&self, node_idx: usize) -> FlatGraphNode<'_> {
        let start = match node_idx {
            0 => 0,
            _ => self.nodes[node_idx - 1],
        };
        let end = self.nodes[node_idx];
        let the_edges = &self.edges[start..end];
        FlatGraphNode { edges: the_edges }
    }

    pub fn build_predecessors(&self) -> Predecessors {
        let mut preds: Vec<Option<Vec<usize>>> = vec![None; self.nodes.len()];
        for source_idx in 0..self.nodes.len() {
            let node = self.get_node(source_idx);
            for edge in node.edges {
                // Ensure we don't add duplicate predecessors if your data has them
                match preds[edge.target].as_mut() {
                    None => {
                        preds[edge.target] = Some(vec![source_idx]);
                    }
                    Some(list) => {
                        list.push(source_idx);
                    }
                }
            }
        }
        // Sort and dedup each predecessor list
        // flatten to skip None's.
        for p in preds.iter_mut().flatten() {
            p.sort_unstable();
            p.dedup();
        }
        preds
    }
}

pub struct FlatGraphNode<'a> {
    pub edges: &'a [Edge],
}

// A graph of which the edges are sorted, it is created by taking an existing graph and sorting it.
// Then only a & to the original graph can be obtained so we can guarantee the sort invariant
#[derive(Debug)]
pub struct SortedGraph {
    graph: Graph,
}

impl SortedGraph {
    // Creates a new graph, taking all edges from the provided graph.
    // This does at the same time remove duplicates
    pub fn new(mut g: Graph) -> SortedGraph {
        g.nodes
            .par_iter_mut()
            //perform a quick check whether there is more than one element
            .for_each(|node| {
                node.edges
                    .sort_unstable_by(|a, b| (a.label, a.target).cmp(&(b.label, b.target)));
                node.edges.dedup_by_key(|e| (e.label, e.target));
            });
        SortedGraph { graph: g }
    }

    // merge the edges from the other sorted graph into the ones of self, modifying self, duplicates are deduplicated
    // If other has more nodes than self, self gets enlarged
    pub fn merge_into_self(&mut self, other: &SortedGraph) {
        // invariants used:
        // each edge list is already a sorted set of edges (no duplicates), this is guaranteed by the new function
        // Same number of nodes

        let self_node_count = self.graph.nodes.len();
        let other_node_count = other.graph.nodes.len();

        if self_node_count < other_node_count {
            // we need to expand self to the size of other
            self.graph.nodes.resize_with(other_node_count, Node::new);
        }

        // We only need to iterate over the nodes of other
        for i in 0..other_node_count {
            let a = &self
                .graph
                .nodes
                .get(i)
                .expect("array resized above, element must exist")
                .edges;
            let b = &other
                .graph
                .nodes
                .get(i)
                .expect("array resized above, element must exist")
                .edges;
            if b.is_empty() {
                // nothing to do
                continue;
            }
            let mut merged_edges = Vec::with_capacity(a.len() + b.len());
            let mut i = 0;
            let mut j = 0;
            while i < a.len() && j < b.len() {
                if (a[i].label, a[i].target) < (b[j].label, b[j].target) {
                    merged_edges.push(a[i].clone());
                    i += 1;
                } else if (b[j].label, b[j].target) < (a[i].label, a[i].target) {
                    merged_edges.push(b[j].clone());
                    j += 1;
                } else {
                    // They are equal: push one, skip both
                    merged_edges.push(a[i].clone());
                    i += 1;
                    j += 1;
                }
            }
            // push the remainders
            for edge in a.iter().skip(i) {
                merged_edges.push(edge.clone());
            }
            for edge in b.iter().skip(j) {
                merged_edges.push(edge.clone());
            }
            merged_edges.shrink_to_fit();
            // now we put the merged ones into self.
            self.graph.nodes[i].edges = merged_edges;
        }
    }

    // merge the edges from the other sorted graph with the ones in self, creating a new graph
    // In the current implementation, it is assumed both graphs have the same number of nodes.
    pub fn merge_equal_size_to_new(one: &SortedGraph, other: &SortedGraph) -> SortedGraph {
        // invariants used:
        // each edge list is already a sorted set of edges (no duplicates), this is guaranteed by the new function
        // Same number of nodes

        assert!(one.graph.nodes.len() == other.graph.nodes.len());

        let mut merged_nodes = Vec::new();
        for (a, b) in one.graph.nodes.iter().zip_eq(other.graph.nodes.iter()) {
            let (a, b) = (&a.edges, &b.edges);
            let mut merged_edges = Vec::with_capacity(a.len() + b.len());
            let mut i = 0;
            let mut j = 0;
            while i < a.len() && j < b.len() {
                if (a[i].label, a[i].target) < (b[j].label, b[j].target) {
                    merged_edges.push(a[i].clone());
                    i += 1;
                } else if (b[j].label, b[j].target) < (a[i].label, a[i].target) {
                    merged_edges.push(b[j].clone());
                    j += 1;
                } else {
                    // They are equal: push one, skip both
                    merged_edges.push(a[i].clone());
                    i += 1;
                    j += 1;
                }
            }
            // push the remainders
            for edge in a.iter().skip(i) {
                merged_edges.push(edge.clone());
            }
            for edge in b.iter().skip(j) {
                merged_edges.push(edge.clone());
            }
            merged_nodes.push(Node {
                edges: merged_edges,
            });
        }
        SortedGraph {
            graph: Graph {
                nodes: merged_nodes,
            },
        }
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}

// --- Helper Functions (IO) ---

/// Reads triples from a stream and executes a callback for each.
/// This decouples the IO logic from the Graph struct, which is more idiomatic in Rust
/// to avoid borrowing issues.
pub fn read_triples_from_stream<R, F>(
    mut r: R,
    reverse_edges: bool,
    mut callback: F,
) -> io::Result<()>
where
    R: Read,
    F: FnMut(NodeIndex, EdgeType, NodeIndex),
{
    let mut line_counter: u64 = 0;

    // Reusable buffer to avoid allocation
    // We need max(5, 4) bytes, so 5 is enough.
    let mut buf = [0u8; 5];

    loop {
        line_counter += 1;

        // Read Subject (Entity)
        let subject = match read_uint_entity(&mut r, &mut buf)? {
            Some(val) => val,
            None => break, // EOF reached cleanly
        };

        // Read Predicate (EdgeType)
        let predicate = read_uint_predicate(&mut r, &mut buf)?;

        // Read Object (Entity)
        let object = match read_uint_entity(&mut r, &mut buf)? {
            Some(val) => val,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF reading object",
                ));
            }
        };

        if reverse_edges {
            callback(object, predicate, subject);
        } else {
            callback(subject, predicate, object);
        }

        if line_counter.is_multiple_of(100_000_000) {
            println!("Read {} triples", line_counter);
        }
    }

    Ok(())
}

// The buffer is used for the reading and guaranteed to contain the bytes as read
pub fn read_uint_entity<R: Read>(r: &mut R, buf: &mut [u8; 5]) -> io::Result<Option<NodeIndex>> {
    // Attempt to read the first byte to detect clean EOF
    let mut first_byte = [0u8; 1];
    match r.read(&mut first_byte) {
        Ok(0) => return Ok(None), // EOF
        Ok(_) => buf[0] = first_byte[0],
        Err(e) => return Err(e),
    }

    // Read remaining 4 bytes
    r.read_exact(&mut buf[1..BYTES_PER_ENTITY])?;

    let mut result: NodeIndex = 0;
    for (i, the_byte) in buf.iter().enumerate() {
        result |= (*the_byte as NodeIndex) << (8 * i);
    }

    Ok(Some(result))
}

pub fn read_uint_predicate<R: Read>(r: &mut R, buf: &mut [u8; 5]) -> io::Result<EdgeType> {
    // Reuse the buffer, but we only need 4 bytes
    r.read_exact(&mut buf[0..BYTES_PER_PREDICATE])?;

    let mut result: EdgeType = 0;
    for (i, the_byte) in buf.iter().take(4).enumerate() {
        result |= (*the_byte as EdgeType) << (8 * i);
    }
    Ok(result)
}

fn write_triple<W: Write>(
    w: &mut W,
    subject: NodeIndex,
    label: EdgeType,
    object: NodeIndex,
) -> io::Result<()> {
    write_uint_entity(w, subject)?;
    write_uint_predicate(w, label)?;
    write_uint_entity(w, object)?;
    Ok(())
}

fn write_uint_entity<W: Write>(w: &mut W, val: NodeIndex) -> io::Result<()> {
    // Extract 5 bytes from the usize
    let mut data = [0u8; BYTES_PER_ENTITY];

    for (i, the_byte) in data.iter_mut().enumerate() {
        *the_byte = (val >> (8 * i)) as u8;
    }
    w.write_all(&data)
}

fn write_uint_predicate<W: Write>(w: &mut W, val: EdgeType) -> io::Result<()> {
    // Extract 4 bytes from the u32
    let mut data = [0u8; BYTES_PER_PREDICATE];

    for (i, the_byte) in data.iter_mut().enumerate() {
        *the_byte = (val >> (8 * i)) as u8;
    }

    w.write_all(&data)
}

pub fn get_graph_triple_count_from_binary_file<P: AsRef<Path>>(
    file: P,
) -> Result<usize, GraphValidationError> {
    let path = file.as_ref();
    let graph_binary_size = fs::metadata(path)?.len() as usize;
    if graph_binary_size.is_multiple_of(BYTES_PER_TRIPLE) {
        return Err(GraphValidationError::NotDivisible {
            num_bytes: (graph_binary_size),
        });
    }
    Ok(graph_binary_size / BYTES_PER_TRIPLE)
}

/// This function optimizes the ordering in the graph, by remapping the edge types, and sorting accordingly.
/// Then, the nodes are sorted lexicographically, with respect to their edge set.
/// All this, is aimed at making get_i_bisimulation faster and less memory consuming
/// This would happen because
/// 1. in the signature tree, there would be longer common prefixes, and
/// 2. as nodes that have similar edges are close to each other, we get better locality in the signature tree
/// 3. as nodes that have the same edge set are right next to each other, we might even be able to skip the signature calculation altogether;
///    after the first one of its kind we only need to check whether the edge set is exactly the same which is very fast.
pub fn optimize_graph_for_bisimulation(mut g: Graph, rel_count: EdgeType) -> FlatGraph {
    remap_edge_types_ascending_frequency(&mut g, rel_count);
    // All edges are now mapped, higher index means less frequent.

    // Step 2: we want to sort all nodes lexicographicallly by their edge type set.
    // however, because edge sets can be quite varying in size, and we expect little gain from the long tail
    // we will only sort up to a certain lexicographic depth.

    // We set this as a multiple of 32, hoping to get better cache alignment,
    let lexicographic_depth = 4 * 32;

    // The graph we have contains duplicates in its edge types, we will create a shadow array to get rid of these to speed up the sort
    // as we now have a fixed number of things

    let shadow_array = build_shadow_array(&g, lexicographic_depth);

    let indices = msd_radix_sort::msd_radix_sort(
        g.get_size(),
        rel_count as usize,
        5000, // This parameter was recommended by gemini, based on an L3 cache size of 256MB
        lexicographic_depth,
        &shadow_array,
    );

    // Now we will create the final FlatGraph with the sorted content.
    // We can allocate all required memory upfront. This completely eliminates
    // reallocation pauses during the loop.
    // first we get the sizes to be allocated
    let node_count = g.nodes.len();
    let edge_count = g.get_total_edge_count();
    let mut fg = FlatGraph {
        nodes: Vec::with_capacity(node_count),
        edges: Vec::with_capacity(edge_count),
    };

    for old_node_id in indices.iter() {
        let node: &mut Node = &mut g.nodes[*old_node_id];
        let edges_in_this_node_count = node.edges.len();
        fg.nodes.push(fg.edges.len() + edges_in_this_node_count);
        fg.edges.append(&mut node.edges);
    }

    // Invert the sorted indices into a "ranks" array to map the targets
    let mut ranks = vec![0; indices.len()];
    for (sorted_position, original_id) in indices.into_iter().enumerate() {
        ranks[original_id] = sorted_position;
    }

    for edge in fg.edges.iter_mut() {
        edge.target = ranks[edge.target];
    }

    fg
}

/// Builds a dense, fixed-width shadow array for fast MSD Radix Sorting.
///
/// * `flat`: Your original CSR-like data structure.
/// * `k`: The maximum depth to truncate the sorting prefix (e.g., 16).
fn build_shadow_array(g: &Graph, k: usize) -> Vec<u32> {
    // A standard offset table has num_sets + 1 entries.

    // Pre-allocate the entire array with the sentinel value.
    // This perfectly handles the padding for sets shorter than `k` upfront.
    let mut shadow_array = vec![msd_radix_sort::EOS_SENTINEL; g.get_size() * k];

    for (node_idx, node) in g.nodes.iter().enumerate() {
        // let start = flat.n[set_id];
        // let end = flat.n[set_id + 1];

        let mut unique_count = 0;
        let mut last_val: Option<u32> = None;

        // Calculate the starting index for this set in the 1D shadow array
        let shadow_base_idx = node_idx * k;

        // Iterate through the original set elements
        for edge in &node.edges {
            // Stop if we have hit the prefix truncation limit
            if unique_count >= k {
                break;
            }

            let val = edge.label;

            // Deduplication: Only write if it differs from the last written value
            if Some(val) != last_val {
                shadow_array[shadow_base_idx + unique_count] = val;
                last_val = Some(val);
                unique_count += 1;
            }
        }
        // Any remaining slots in the `k`-block are already correctly
        // filled with EOS_SENTINEL from the initial vector allocation.
    }

    shadow_array
}

fn remap_edge_types_ascending_frequency(g: &mut Graph, rel_count: EdgeType) {
    // Step1: we remap the edge types by global frequency, most frequent type first
    // compute global frequencies of edge types
    let mut absolute_frequencies: FxHashMap<EdgeType, u64> = FxHashMap::default();
    g.nodes.iter().flat_map(|n| &n.edges).for_each(|edge| {
        absolute_frequencies
            .entry(edge.label)
            .and_modify(|e| *e += 1)
            .or_insert(1);
    });

    let mut types_by_descending_frequency: Vec<EdgeType> = (0..rel_count).collect();
    types_by_descending_frequency.sort_unstable_by(|t1: &EdgeType, t2: &EdgeType| {
        absolute_frequencies
            .get(t1)
            .unwrap_or(&0) // This happens if an edge type is not actually used in the graph
            .cmp(
                absolute_frequencies.get(t2).unwrap_or(&0), // This happens if an edge type is not actually used in the graph
            )
            .reverse()
    });

    drop(absolute_frequencies);

    let mut types_by_descending_frequency_mapping: Vec<EdgeType> = (0..rel_count).collect();
    for (idx, rel) in types_by_descending_frequency.iter().enumerate() {
        types_by_descending_frequency_mapping[*rel as usize] = idx as u32;
    }

    for edge in g.nodes.iter_mut().flat_map(|n| &mut n.edges) {
        edge.label = types_by_descending_frequency_mapping[edge.label as usize];
    }

    // TODO: it might be that we return one of these, or give it to a callback se we can free up the memory. To be decided.
    drop(types_by_descending_frequency);
    drop(types_by_descending_frequency_mapping);

    // Now that all are mapped, we sort them with natural ordering
    // We do a lexicographic ordering with the edge target, this might help a bit if we want to later figure out that two nodes have exactly the same set of edges
    // It might be even better to do the sorting of the targets by in-degree, but we do nto have that at this point.
    for node in g.nodes.iter_mut() {
        node.edges
            .sort_unstable_by(|n1, n2| n1.label.cmp(&n2.label).then(n1.target.cmp(&n2.target)));
    }
}
