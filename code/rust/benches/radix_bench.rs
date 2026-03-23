use criterion::{Criterion, black_box, criterion_group, criterion_main};
use multi_summaries::signature_tree::RadixTree;
use std::collections::HashMap;

// A helper to generate our massive dataset outside the timing loop
fn generate_dataset(size: usize) -> Vec<Vec<char>> {
    let mut dataset: Vec<Vec<char>> = Vec::with_capacity(size);
    let bases = [
        "https://internal-api.production.us-east-1.company.com/v3/service/module/users/profile/data/?user_id=",
        "https://internal-api.production.us-east-1.company.com/v3/service/module/orders/history/data/?user_id=",
        "https://internal-api.production.us-east-1.company.com/v3/service/module/billing/invoices/data/?user_id=",
    ];

    for i in 0..size {
        let base = bases[i % bases.len()];
        let id = (i % 5000).to_string();
        let mut seq: Vec<char> = base.chars().collect();
        seq.extend(id.chars());
        dataset.push(seq);
    }
    dataset
}

fn bench_data_structures(c: &mut Criterion) {
    let dataset_size = 50_000; // Adjusted for a reasonable bench time
    let dataset = generate_dataset(dataset_size);

    // Create a benchmark group to compare them side-by-side
    let mut group = c.benchmark_group("Unique Sequences (50k items)");

    // 1. Benchmark the HashMap
    group.bench_function("HashMap", |b| {
        // b.iter() is the actual measured loop. Everything outside it is setup.
        b.iter(|| {
            let mut map: HashMap<Vec<char>, Vec<usize>> = HashMap::new();
            for (idx, seq) in dataset.iter().enumerate() {
                // black_box prevents the compiler from optimizing away our work
                // since we aren't "returning" or printing the map.
                map.entry(black_box(seq.clone())).or_default().push(idx);
            }
            black_box(map);
        })
    });

    // 2. Benchmark the Radix Tree Arena
    group.bench_function("RadixTreeArena", |b| {
        b.iter(|| {
            // Re-allocate inside the iter so we get a fair "cold start" comparison
            // just like the HashMap.
            // Note: you can use your custom const generic sizes here if you implemented them!
            let mut arena = RadixTree::<char, 16, 4, 4>::new();
            for (idx, seq) in dataset.iter().enumerate() {
                arena.insert(black_box(seq.iter().cloned()), idx);
            }
            black_box(arena);
        })
    });

    group.finish();
}

// Wire up the Criterion macros to run the bench
criterion_group!(benches, bench_data_structures);
criterion_main!(benches);
