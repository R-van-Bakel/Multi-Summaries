// This code was first created using gemini with the following prompts:
// 1. Is there a way to do the lexicographic sorting with a MSD radix sort? There are a lot of sets to be inserted (billions), but the elements inside are from a more limited alphabet (max 100K). The alfabet consists of contiguous integers from 0 until alphabet_size.
// 2. Can you implement only the radix sort algorithm in rust, including all suggested optimizations. I think it is a good idea if the function takes a closure/callback/lambda that is given an index of a set, and the current index in the strings; this callback then returns an integer value, indicating the bucket for the radix sort.
// ... Feel free to suggest a better alternative. As final result, this function returns an array, that for each of my original elements indicates where it should go in the lexicographical ordering.

// Then, after reviewing the first version of the code, I found out it could be further optimized by first creating a shadow graph, that can then be used instead of the real graph.
// With that informaiton, the following prompt was used:
// 3. Given I have now pre-sorted the shadow array, Can you re-implement the msd_radix_sort to make use of this?

use std::cmp::Ordering;

pub const EOS_SENTINEL: u32 = u32::MAX;

/// Sorts billion-scale integer sets lexicographically using an in-place MSD Radix Sort.
///
/// Returns an array of indices that tells you "which original set should be placed 0th,  1st, 2nd, 3rd,"
///
/// * `num_sets`: The total number of sets (matches flat.n.len() - 1).
/// * `alphabet_size`: The maximum possible integer value in your sets (e.g., 100_000).
/// * `threshold`: The slice size at which the algorithm switches to QuickSort (e.g., 2000).
/// * `k`: The truncation limit / exact width of each set in the shadow array.
/// * `shadow_array`: The flat 1D array containing the deduplicated, padded elements.
pub fn msd_radix_sort(
    num_sets: usize,
    alphabet_size: usize,
    threshold: usize,
    k: usize,
    shadow_array: &[u32],
) -> Vec<usize> {
    // Initialize our array of pointers/IDs. We will only sort this array.
    let mut indices: Vec<usize> = (0..num_sets).collect();

    // Call the recursive sorting function starting at depth 0
    msd_radix_sort_recursive(&mut indices, alphabet_size, threshold, k, 0, shadow_array);

    indices
}

fn msd_radix_sort_recursive(
    indices: &mut [usize],
    alphabet_size: usize,
    threshold: usize,
    k: usize,
    depth: usize,
    shadow_array: &[u32],
) {
    let len = indices.len();

    // OPTIMIZATION 1: Truncation limit reached.
    // All items in this bucket share the exact same prefix up to length `k`.
    // We consider them sorted and stop recursing.
    if len <= 1 || depth >= k {
        return;
    }

    // OPTIMIZATION 2: Thresholding to Comparison Sort
    if len <= threshold {
        indices.sort_unstable_by(|&a, &b| {
            // We only need to check depths from current up to k
            for d in depth..k {
                let va = shadow_array[a * k + d];
                let vb = shadow_array[b * k + d];

                if va != vb {
                    // EOS must sort *before* any valid alphabet character.
                    // Because u32::MAX is numerically the largest, we must explicitly
                    // handle it, otherwise shorter sets would end up at the back.
                    if va == EOS_SENTINEL {
                        return Ordering::Less;
                    }
                    if vb == EOS_SENTINEL {
                        return Ordering::Greater;
                    }

                    return va.cmp(&vb);
                }
            }
            Ordering::Equal
        });
        return;
    }

    // OPTIMIZATION 3: In-Place Permutation (American Flag Sort)
    let num_buckets = alphabet_size + 2;
    let mut counts = vec![0; num_buckets];

    // Pass 1: Count frequencies directly from the 1D shadow array
    for &idx in indices.iter() {
        let val = shadow_array[idx * k + depth];
        let bucket = if val == EOS_SENTINEL {
            0
        } else {
            (val + 1) as usize
        };
        counts[bucket] += 1;
    }

    // Pass 2: Calculate prefix sums (bucket boundaries)
    let mut offsets = vec![0; num_buckets];
    let mut sum = 0;
    for i in 0..num_buckets {
        offsets[i] = sum;
        sum += counts[i];
    }

    // Pass 3: In-place cyclic permutation
    let mut current = offsets.clone();

    for b in 0..=alphabet_size {
        while current[b] < offsets[b + 1] {
            let idx = indices[current[b]];
            let val = shadow_array[idx * k + depth];
            let target_bucket = if val == EOS_SENTINEL {
                0
            } else {
                (val + 1) as usize
            };

            if target_bucket == b {
                current[b] += 1;
            } else {
                let target_idx = current[target_bucket];
                indices.swap(current[b], target_idx);
                current[target_bucket] += 1;
            }
        }
    }

    // Pass 4: Recursion
    // Start at bucket 1 to skip the EOS bucket
    for b in 1..=alphabet_size {
        let start = offsets[b];
        let end = offsets[b + 1];

        if end - start > 1 {
            msd_radix_sort_recursive(
                &mut indices[start..end],
                alphabet_size,
                threshold,
                k,
                depth + 1,
                shadow_array,
            );
        }
    }
}
