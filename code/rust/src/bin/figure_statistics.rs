use clap::Parser;
use fxhash::{FxHashMap, FxHashSet};
use multi_summaries::bisimulator::GlobalBlockIndex;
use multi_summaries::graph::NodeIndex;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, Read, Result},
    mem,
    path::PathBuf,
};

// TODO this is lazily copied from main.rs; they should depend on a shared definition instead
#[derive(Serialize, Deserialize)]
pub struct BisimulationStatistics {
    singletons_condensed: Vec<usize>,
    singletons_uncondensed: Vec<usize>,
    blocks_quotient: Vec<usize>,
    blocks_condensed: Vec<usize>,
    blocks_uncondensed: Vec<usize>,
    refines_edges_condensed: Vec<usize>,
    data_edges_quotient: Vec<usize>,
    data_edges_condensed: Vec<usize>,
    data_edges_uncondensed: Vec<usize>,
}

// #[derive(Serialize, Deserialize, Debug)]
// struct StatifiedBlockSizeCount {
//     level: usize,
//     block_size: usize,
//     size_count: usize,
// }

// Level, block size, size count
type SizeCount = (usize, usize, usize);

#[derive(Serialize, Deserialize)]
struct FigureStatistics {
    splitting_blocks: Vec<usize>,
    vertices_in_splitting_blocks: Vec<usize>,
    singletons: Vec<usize>,
    sizes_counts: Vec<SizeCount>,
}

#[derive(Parser, Debug)]
struct Cli {
    /// The path to the directory containing the bisimulation output
    bisimulation_path: PathBuf,
}

fn get_size_counts_from_block_sizes(
    block_sizes: &FxHashMap<GlobalBlockIndex, NodeIndex>,
) -> std::collections::HashMap<usize, usize, std::hash::BuildHasherDefault<fxhash::FxHasher>> {
    block_sizes.values().fold(
        FxHashMap::default(),
        |mut acc: std::collections::HashMap<
            usize,
            usize,
            std::hash::BuildHasherDefault<fxhash::FxHasher>,
        >,
         &v| {
            *acc.entry(v).or_insert(0) += 1;
            acc
        },
    )
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let input_path = &args.bisimulation_path;

    let node_idx_bytes_size = mem::size_of::<NodeIndex>();
    let global_block_idx_bytes_size = mem::size_of::<GlobalBlockIndex>();

    // 1. Load the pre-computed singleton counts
    let bisimulation_statistics_file_path = input_path.join("statistics.json");
    let bisimulation_statistics: BisimulationStatistics =
        serde_json::from_reader(File::open(bisimulation_statistics_file_path)?)?;
    let singletons: Vec<usize> = bisimulation_statistics.singletons_condensed.clone();

    let last_level = singletons.len() - 1;

    let mut figure_statistics = FigureStatistics {
        splitting_blocks: vec![0; singletons.len()],
        vertices_in_splitting_blocks: vec![0; singletons.len()],
        singletons: singletons,
        sizes_counts: Vec::new(),
    };

    // 2. Read the final outcome and store the block sizes
    let final_node_to_block_file_path = input_path.join("final_node_to_block");
    let mut reader = BufReader::new(File::open(final_node_to_block_file_path)?);
    let mut buf = vec![0u8; node_idx_bytes_size + global_block_idx_bytes_size];
    let mut block_sizes: FxHashMap<GlobalBlockIndex, NodeIndex> = FxHashMap::default();
    let block_offset = node_idx_bytes_size; // Alias
    let block_end = block_offset + global_block_idx_bytes_size;

    loop {
        match reader.read_exact(&mut buf) {
            Ok(()) => {
                let block_bytes = &buf[block_offset..block_end];
                let block = usize::from_be_bytes(block_bytes.try_into().unwrap());

                // If the block is new (i.e. not in the map) add it and set its size to 1, else increment the size by 1
                block_sizes
                    .entry(block)
                    .and_modify(|block_size| *block_size += 1)
                    .or_insert(1);
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }
    // println!("{:?}", block_sizes);

    // Count the block sizes
    let counter = get_size_counts_from_block_sizes(&block_sizes);

    // Store the current size in figure_statistics
    for (block_size, size_count) in counter {
        figure_statistics
            .sizes_counts
            .push((last_level, block_size, size_count));
    }
    // println!("{:?}", figure_statistics.sizes_counts);

    // 3. Iteratively load refines edges and update statistics
    let refines_dir = input_path.join("refines/");
    for i in (1..=last_level).rev() {
        let refines_file_path = refines_dir.join(format!("refines_{}", i));
        let mut reader = BufReader::new(File::open(refines_file_path)?);
        let mut buf = vec![0u8; global_block_idx_bytes_size + global_block_idx_bytes_size];
        let mut target_blocks: FxHashSet<GlobalBlockIndex> = FxHashSet::default();

        loop {
            match reader.read_exact(&mut buf) {
                Ok(()) => {
                    let (source_bytes, target_bytes) = buf.split_at(global_block_idx_bytes_size);

                    let source_block_id = usize::from_be_bytes(source_bytes.try_into().unwrap());
                    let target_block_id = usize::from_be_bytes(target_bytes.try_into().unwrap());

                    target_blocks.insert(target_block_id);

                    // Update block_size via the refines map
                    if let Some(block_size) = block_sizes.remove(&source_block_id) {
                        *block_sizes.entry(target_block_id).or_insert(0) += block_size;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        // Update splitting blocks statistic
        figure_statistics.splitting_blocks[i - 1] = target_blocks.len();

        // Update vertices splitting blocks statistic
        for block_id in target_blocks {
            figure_statistics.vertices_in_splitting_blocks[i - 1] += block_sizes[&block_id];
        }

        let counter = get_size_counts_from_block_sizes(&block_sizes);
        // Store the current size in figure_statistics
        for (block_size, size_count) in counter {
            figure_statistics
                .sizes_counts
                .push((i, block_size, size_count));
        }
    }

    let figure_statistics_file_path = input_path.join("figure_statistics.json");
    let figure_statistics_file = File::create(figure_statistics_file_path)?;
    serde_json::to_writer_pretty(figure_statistics_file, &figure_statistics)?;
    Ok(())
}
