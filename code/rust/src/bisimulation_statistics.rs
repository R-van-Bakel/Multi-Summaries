use std::io::Write;

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::StaticFormatDescription};
use time_macros::format_description;

use crate::bisimulator::{DataEdgeCounter, FullBisimulationState};

pub static FMT: StaticFormatDescription = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
);

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

pub struct BorrowedBisimulationStatistics<'a> {
    pub singletons_condensed: &'a Vec<usize>,
    pub singletons_uncondensed: &'a Vec<usize>,
    pub blocks_quotient: &'a Vec<usize>,
    pub blocks_condensed: &'a Vec<usize>,
    pub blocks_uncondensed: &'a Vec<usize>,
    pub refines_edges_condensed: &'a Vec<usize>,
    pub data_edges_quotient: &'a Vec<usize>,
    pub data_edges_condensed: &'a Vec<usize>,
    pub data_edges_uncondensed: &'a Vec<usize>,
}

impl BisimulationStatistics {
    pub fn new() -> Self {
        BisimulationStatistics {
            singletons_condensed: Vec::new(),
            singletons_uncondensed: Vec::new(),
            blocks_quotient: Vec::new(),
            blocks_condensed: Vec::new(),
            blocks_uncondensed: Vec::new(),
            refines_edges_condensed: Vec::new(),
            data_edges_quotient: Vec::new(),
            data_edges_condensed: Vec::new(),
            data_edges_uncondensed: Vec::new(),
        }
    }

    pub fn get_borrowed_view(&self) -> BorrowedBisimulationStatistics<'_> {
        let singletons_condensed = &self.singletons_condensed;
        let singletons_uncondensed = &self.singletons_uncondensed;
        let blocks_quotient = &self.blocks_quotient;
        let blocks_condensed = &self.blocks_condensed;
        let blocks_uncondensed = &self.blocks_uncondensed;
        let refines_edges_condensed = &self.refines_edges_condensed;
        let data_edges_quotient = &self.data_edges_quotient;
        let data_edges_condensed = &self.data_edges_condensed;
        let data_edges_uncondensed = &self.data_edges_uncondensed;
        BorrowedBisimulationStatistics {
            singletons_condensed,
            singletons_uncondensed,
            blocks_quotient,
            blocks_condensed,
            blocks_uncondensed,
            refines_edges_condensed,
            data_edges_quotient,
            data_edges_condensed,
            data_edges_uncondensed,
        }
    }

    pub fn add_level(
        &mut self,
        bisimulation_state: &FullBisimulationState<impl Write, impl Write>,
    ) {
        let now = OffsetDateTime::now_local()
            .expect("time could not get the local time")
            .format(&FMT)
            .unwrap();

        let last_singletons_uncondensed = self.singletons_uncondensed.last().copied().unwrap_or(0);
        let last_blocks_condensed = self
            .blocks_condensed
            .last()
            .copied()
            .unwrap_or(bisimulation_state.current_outcome.total_blocks());
        let last_blocks_uncondensed = self.blocks_uncondensed.last().copied().unwrap_or(0);
        let refines_edges_condensed = self.refines_edges_condensed.last().copied().unwrap_or(0);

        self.singletons_uncondensed
            .push(last_singletons_uncondensed + bisimulation_state.current_outcome.singletons());
        self.blocks_condensed
            .push(last_blocks_condensed + bisimulation_state.shared_state.refines_edge_count());
        self.blocks_uncondensed
            .push(last_blocks_uncondensed + bisimulation_state.current_outcome.total_blocks());
        self.refines_edges_condensed
            .push(refines_edges_condensed + bisimulation_state.shared_state.refines_edge_count());

        self.singletons_condensed
            .push(bisimulation_state.current_outcome.singletons());
        self.blocks_quotient
            .push(bisimulation_state.current_outcome.total_blocks());

        println!(
            "{} - After computing {:>4}-bisimulation --> Dirty blocks: {:<10}, singletons: {:<10}, blocks {:<10}, blocks (condensed) {:<10}, singletons (uncondensed) {:<10} blocks (uncondensed) {:<10}, refines edges ((un)condensed) {:<10}",
            now,
            bisimulation_state.shared_state.i - 1,
            bisimulation_state.current_outcome.dirty_blocks.len(),
            self.singletons_condensed.last().unwrap(),
            self.blocks_quotient.last().unwrap(),
            self.blocks_condensed.last().unwrap(),
            self.singletons_uncondensed.last().unwrap(),
            self.blocks_uncondensed.last().unwrap(),
            self.refines_edges_condensed.last().unwrap(),
        );
    }

    pub fn add_aggregate_data_edges(&mut self, aggregate_counter: DataEdgeCounter) {
        assert!(self.data_edges_condensed.is_empty() && self.data_edges_uncondensed.is_empty());
        self.data_edges_condensed = aggregate_counter.condensed_counts;
        self.data_edges_uncondensed = aggregate_counter.uncondensed_counts
    }
}
