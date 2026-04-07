// Code generated with gemini 3.1 Pro with the following prompt:
// I want to build a data structure and algorithm in rust to do the following: given an Iterator<Iterator<T>> with T ordinal. Think of the inner Iterator as a string (that contains more complex structs instead of characters). I need to find the unique strings in the outer Iterator, and their corresponding index in the outer Iterator. A trivial solution is a hashmap in which I maintain a counter with the number of encounters, but that is not efficient because it hashes the whole string while just finding one difference is sufficient. In the end I need to be able to iterate over the data structure and at each step get (an Iterator over) the string and the set of indices that had that string. A radix tree seems like a good solution. Can you write me an implementation of the data structure and the iterator trait? Optimize where possible, unsafe and a specialized vector type to store small vectors can be used.
// The into_iter needed several further requests until it looked reasonable
// Then, this was changed to support const generics for the sizes,
// and to support an tree reduce the number of allocations and have things closer in memory
// Further comments, tests and benchmarks were added afterwards
// Later, the use of clone was reduced/removed
// Even later: analyzing, it became clear that the each node kept its segment memory allocated for the longest prefix it ever contained.
// as the segment will never grow again, it makes sense to shrink it to fit its memory requirements. This might also make it fit into SEG_CAP.

use std::usize;

use smallvec::{Array, SmallVec};

// A node in the Arena-backed Radix Tree, parameterized by SmallVec inline capacities.
#[derive(Debug)]
struct RadixNode<T, const SEG_CAP: usize, const IDX_CAP: usize, const CHILD_CAP: usize> {
    // The compressed path segment this edge represents.
    pub segment: SmallVec<[T; SEG_CAP]>,
    // Indices from the outer iterator where this exact sequence ends.
    pub indices: SmallVec<[usize; IDX_CAP]>,
    // Children, kept sorted by the first element of their segment for binary search.
    pub children: SmallVec<[usize; CHILD_CAP]>,
}

/// A radix tree keeping a segment in each node.
/// It also keeps the indices of things added that have the string to that point.
/// The children are a sorted on the first element of the children. That way we can use binary search.
/// The tree is intended for reuse by calling reset, which empties the tree, but does not release the memory.
/// The nodes are stored in a contiguous array for
pub struct RadixTree<
    T,
    const SEG_CAP: usize = 4,
    const IDX_CAP: usize = 2,
    const CHILD_CAP: usize = 4,
> {
    /// The tree passed the const generics down to the nodes.
    nodes: Vec<RadixNode<T, SEG_CAP, IDX_CAP, CHILD_CAP>>,
}

impl<T: Ord + Clone, const SEG_CAP: usize, const IDX_CAP: usize, const CHILD_CAP: usize>
    RadixTree<T, SEG_CAP, IDX_CAP, CHILD_CAP>
{
    /// Make a new RadixTree without any memory pre-allocated.
    pub fn new() -> Self {
        let mut tree = RadixTree { nodes: Vec::new() };
        tree.reset();
        tree
    }

    /// Make a new RadixTree with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let mut tree = RadixTree {
            nodes: Vec::with_capacity(capacity),
        };
        tree.reset();
        tree
    }

    /// Removes all data in the tree, but does *not* release the memory ares used for the nodes.
    pub fn reset(&mut self) {
        self.nodes.clear();
        self.nodes.push(RadixNode {
            segment: SmallVec::new(),
            indices: SmallVec::new(),
            children: SmallVec::new(),
        });
    }

    fn add_node(&mut self, node: RadixNode<T, SEG_CAP, IDX_CAP, CHILD_CAP>) -> usize {
        let idx = self.nodes.len();
        self.nodes.push(node);
        idx
    }

    // Insert something into the tree. The outer_index will be added to the set of usize associated with this sequence.
    pub fn insert(&mut self, mut iter: impl Iterator<Item = T>, outer_index: usize) {
        let mut current_idx = 0; // Start at root
        let mut current_item = iter.next();

        'outer: while let Some(item) = current_item {
            // We temporarily borrow the tree to find the child's index.
            // Notice how this borrow ends immediately after the statement!
            let child_search = self.nodes[current_idx]
                .children
                .binary_search_by(|&child_idx| self.nodes[child_idx].segment[0].cmp(&item));

            match child_search {
                Ok(child_pos) => {
                    // Extract the index of the child we need to visit
                    let child_idx = self.nodes[current_idx].children[child_pos];

                    let mut match_len = 1;
                    let mut mismatch_val = None;

                    // Traverse the compressed path of the child
                    for i in 1..self.nodes[child_idx].segment.len() {
                        if let Some(val) = iter.next() {
                            if val == self.nodes[child_idx].segment[i] {
                                match_len += 1;
                            } else {
                                mismatch_val = Some(val);
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    if match_len < self.nodes[child_idx].segment.len() {
                        // The path diverged. Split the node.
                        self.split_node(child_idx, match_len);

                        if let Some(val) = mismatch_val {
                            current_item = Some(val);
                            current_idx = child_idx; // Truncated node acts as intermediate router
                            continue 'outer;
                        } else {
                            self.nodes[child_idx].indices.push(outer_index);
                            return;
                        }
                    } else {
                        // Sequence matched the whole segment; move down
                        current_idx = child_idx;
                        current_item = iter.next();
                    }
                }
                Err(insert_pos) => {
                    // No child starts with this item. Create a new one.
                    let mut new_segment = SmallVec::new();
                    new_segment.push(item);
                    new_segment.extend(iter);

                    let new_node_idx = self.add_node(RadixNode {
                        segment: new_segment,
                        indices: smallvec::smallvec![outer_index],
                        children: SmallVec::new(),
                    });

                    self.nodes[current_idx]
                        .children
                        .insert(insert_pos, new_node_idx);
                    return;
                }
            }
        }

        self.nodes[current_idx].indices.push(outer_index);
    }

    fn split_node(&mut self, child_idx: usize, match_len: usize) {
        // 1 & 3. Drain the suffix out of the original segment.
        // This transfers ownership of the elements into `new_segment`
        // AND automatically truncates the original segment down to `match_len`.
        let new_segment: SmallVec<_> = self.nodes[child_idx].segment.drain(match_len..).collect();

        // Free the memory of the original segment, potentially moving it back inline!
        self.nodes[child_idx].segment.shrink_to_fit();

        // 2. Steal indices and children from the original node
        let indices = std::mem::take(&mut self.nodes[child_idx].indices);
        let children = std::mem::take(&mut self.nodes[child_idx].children);

        // 4. Create the new lower node and add it to the arena
        let new_node_idx = self.add_node(RadixNode {
            segment: new_segment,
            indices,
            children,
        });

        // 5. Link the truncated upper node to the new lower node
        self.nodes[child_idx].children.push(new_node_idx);
    }

    pub fn iter(&self) -> RadixTreeArenaIter<'_, T, SEG_CAP, IDX_CAP, CHILD_CAP> {
        RadixTreeArenaIter {
            arena: self,
            stack: vec![(0, 0)], // Start at root (idx 0) with a path length of 0
            current_path: Vec::with_capacity(256), // Pre-allocate a nice chunk of RAM
        }
    }

    pub fn get_unique_signature_count(&self) -> UniqueSignatureCount {
        let mut count = 0;

        // Count how many nodes actually terminate a sequence.
        // We short-circuit and exit the loop immediately if we hit 2.
        for node in &self.nodes {
            if !node.indices.is_empty() {
                count += 1;
                if count > 1 {
                    return UniqueSignatureCount::MORE;
                }
            }
        }

        match count {
            0 => UniqueSignatureCount::ZERO,
            1 => UniqueSignatureCount::ONE,
            _ => UniqueSignatureCount::MORE, // Handled by the short-circuit, but satisfies the compiler
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum UniqueSignatureCount {
    ZERO,
    ONE,
    MORE,
}

pub struct RadixTreeArenaIter<
    'a,
    T,
    const SEG_CAP: usize,
    const IDX_CAP: usize,
    const CHILD_CAP: usize,
> where
    [T; SEG_CAP]: Array<Item = T>,
    [usize; IDX_CAP]: Array<Item = usize>,
    [usize; CHILD_CAP]: Array<Item = usize>,
{
    arena: &'a RadixTree<T, SEG_CAP, IDX_CAP, CHILD_CAP>,
    // Stack now stores: (node_idx, length_of_path_before_this_node)
    stack: Vec<(usize, usize)>,
    // A single, reusable buffer for the current traversal state
    current_path: Vec<T>,
}

impl<'a, T: Clone, const SEG_CAP: usize, const IDX_CAP: usize, const CHILD_CAP: usize> Iterator
    for RadixTreeArenaIter<'a, T, SEG_CAP, IDX_CAP, CHILD_CAP>
where
    [T; SEG_CAP]: Array<Item = T>,
    [usize; IDX_CAP]: Array<Item = usize>,
    [usize; CHILD_CAP]: Array<Item = usize>,
{
    type Item = (Vec<T>, &'a [usize]);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((node_idx, path_len)) = self.stack.pop() {
            let node = &self.arena.nodes[node_idx];

            // 1. Backtrack! Shrink our single buffer back to the parent's length
            self.current_path.truncate(path_len);

            // 2. Add this node's segment (This still requires cloning the characters
            // from the arena into our buffer, which is unavoidable)
            self.current_path.extend(node.segment.iter().cloned());

            // 3. Push children with the CURRENT length so they know where to backtrack to
            let new_len = self.current_path.len();
            for &child_idx in node.children.iter().rev() {
                self.stack.push((child_idx, new_len)); // NO CLONES HERE! O(1) push!
            }

            if !node.indices.is_empty() {
                // 4. We only clone the path ONCE at the very end to hand it to the user
                return Some((self.current_path.clone(), &node.indices));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type CharArena = RadixTree<char, 8, 2, 4>;

    #[test]
    fn test_zero_signatures() {
        let tree = CharArena::new();
        assert_eq!(
            tree.get_unique_signature_count(),
            UniqueSignatureCount::ZERO
        );
    }

    #[test]
    fn test_one_signature() {
        let mut tree = CharArena::new();
        tree.insert(vec!['a', 'p', 'p', 'l', 'e'].into_iter(), 0);

        assert_eq!(tree.get_unique_signature_count(), UniqueSignatureCount::ONE);
    }

    #[test]
    fn test_one_signature_with_duplicates() {
        let mut tree = CharArena::new();
        let seq = vec!['a', 'p', 'p', 'l', 'e'];

        // Insert the exact same sequence 3 times
        tree.insert(seq.clone().into_iter(), 0);
        tree.insert(seq.clone().into_iter(), 1);
        tree.insert(seq.clone().into_iter(), 2);

        // Should still only be ONE unique signature
        assert_eq!(tree.get_unique_signature_count(), UniqueSignatureCount::ONE);
    }

    #[test]
    fn test_more_signatures_diverging() {
        let mut tree = CharArena::new();
        tree.insert(vec!['a', 'p', 'p', 'l', 'e'].into_iter(), 0);
        tree.insert(vec!['b', 'a', 'n', 'a', 'n', 'a'].into_iter(), 1);

        assert_eq!(
            tree.get_unique_signature_count(),
            UniqueSignatureCount::MORE
        );
    }

    #[test]
    fn test_more_signatures_prefix_split() {
        let mut tree = CharArena::new();
        tree.insert(vec!['a', 'p', 'p', 'l', 'e'].into_iter(), 0);
        // "app" is a prefix of "apple", so it splits the node.
        // Both are unique sequences!
        tree.insert(vec!['a', 'p', 'p'].into_iter(), 1);

        assert_eq!(
            tree.get_unique_signature_count(),
            UniqueSignatureCount::MORE
        );
    }

    #[test]
    fn test_empty_sequence() {
        let mut tree = CharArena::new();
        // Inserting an empty iterator
        tree.insert(std::iter::empty(), 0);

        assert_eq!(tree.get_unique_signature_count(), UniqueSignatureCount::ONE);
    }

    #[test]
    fn benchmark_tree_vs_hashmap() {
        use std::collections::HashMap;
        use std::time::Instant;

        // 1. Generate a large synthetic dataset
        let mut dataset: Vec<Vec<char>> = Vec::with_capacity(50_000);
        let bases = [
            "http://api.service.com/v1/users/",
            "http://api.service.com/v1/posts/",
            "http://api.service.com/v2/config/",
        ];

        for i in 0..50_000 {
            let base = bases[i % bases.len()];
            // Create some duplicates by heavily reusing IDs 0-100
            let id = (i % 100).to_string();
            let mut seq: Vec<char> = base.chars().collect();
            seq.extend(id.chars());
            dataset.push(seq);
        }

        // --- BENCHMARK HASHMAP ---
        let hash_start = Instant::now();
        let mut map: HashMap<Vec<char>, Vec<usize>> = HashMap::new();

        for (idx, seq) in dataset.iter().enumerate() {
            // HashMap requires hashing the whole string and cloning it for the key
            map.entry(seq.clone()).or_default().push(idx);
        }
        let hash_duration = hash_start.elapsed();

        // --- BENCHMARK RADIX TREE ARENA ---
        let tree_start = Instant::now();
        let mut tree = CharArena::with_capacity(1024);

        for (idx, seq) in dataset.iter().enumerate() {
            // Radix Tree only iterates characters and stops early on divergence
            tree.insert(seq.iter().cloned(), idx);
        }
        let tree_duration = tree_start.elapsed();

        // Print results
        println!("======================================");
        println!("HashMap Time:      {:?}", hash_duration);
        println!("Radix Arena Time:  {:?}", tree_duration);
        println!("======================================");

        // Assert they found the same number of unique signatures
        let unique_in_map = map.len();
        let mut unique_in_tree = 0;
        for _ in tree.iter() {
            unique_in_tree += 1;
        }
        assert_eq!(unique_in_map, unique_in_tree);
    }

    #[test]
    fn benchmark_tree_vs_hashmap_large() {
        use std::collections::HashMap;
        use std::time::Instant;

        println!("Generating massive dataset...");
        let dataset_size = 500_000;
        let mut dataset: Vec<Vec<char>> = Vec::with_capacity(dataset_size);

        // Very long common prefixes to force the HashMap to do extra hashing work
        let bases = [
            "https://internal-api.production.us-east-1.company.com/v3/service/module/users/profile/data/?user_id=",
            "https://internal-api.production.us-east-1.company.com/v3/service/module/orders/history/data/?user_id=",
            "https://internal-api.production.us-east-1.company.com/v3/service/module/billing/invoices/data/?user_id=",
        ];

        for i in 0..dataset_size {
            let base = bases[i % bases.len()];
            // Create a mix of duplicates and unique tails
            let id = (i % 5000).to_string();
            let mut seq: Vec<char> = base.chars().collect();
            seq.extend(id.chars());
            dataset.push(seq);
        }
        println!("Dataset generated. Starting benchmark...");

        // --- BENCHMARK HASHMAP ---
        let hash_start = Instant::now();
        let mut map: HashMap<Vec<char>, Vec<usize>> = HashMap::new();

        for (idx, seq) in dataset.iter().enumerate() {
            // HashMap requires cloning the whole sequence to store as a key if it's new
            map.entry(seq.clone()).or_default().push(idx);
        }
        let hash_duration = hash_start.elapsed();
        println!(
            "Print map len and unique key count so it does not compile it out: {} - {}",
            map.len(),
            map.iter().count()
        );

        // --- BENCHMARK RADIX TREE ARENA ---
        let tree_start = Instant::now();
        // Bumped capacities slightly to handle the massive dataset efficiently
        let mut tree = RadixTree::<char, 16, 4, 4>::with_capacity(1024);

        for (idx, seq) in dataset.iter().enumerate() {
            tree.insert(seq.iter().cloned(), idx);
        }
        let tree_duration = tree_start.elapsed();
        println!(
            "Print tree len and unique item count so it does not compile it out: {} - {}",
            tree.nodes.len(),
            tree.iter().count()
        );

        println!("======================================");
        println!("Items processed:   {}", dataset_size);
        println!("HashMap Time:      {:?}", hash_duration);
        println!("Radix Arena Time:  {:?}", tree_duration);
        println!("======================================");

        let unique_in_map = map.len();
        let mut unique_in_tree = 0;
        for _ in tree.iter() {
            unique_in_tree += 1;
        }
        assert_eq!(unique_in_map, unique_in_tree);
    }
}
