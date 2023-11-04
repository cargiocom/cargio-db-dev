use std::collections::BTreeMap;

use casper_types::{bytesrepr::ToBytes, ExecutionResult};
use serde::{Deserialize, Serialize};

use super::Error;

#[cfg(not(test))]
pub(crate) const CHUNK_SIZE_BYTES: usize = 8 * 1024 * 1024;
#[cfg(test)]
pub(crate) const CHUNK_SIZE_BYTES: usize = 20;
const LAST_ELEM_INDEX_IN_CHUNK: usize = CHUNK_SIZE_BYTES - 1;
const FLOAT_TOLERANCE: f64 = 0.1;

#[inline]
pub(crate) fn chunk_count_after_partition(data_size: usize) -> usize {
    (data_size + LAST_ELEM_INDEX_IN_CHUNK) / CHUNK_SIZE_BYTES
}

pub(crate) fn summarize_map(map: &BTreeMap<usize, usize>) -> CollectionStatistics {
    let elem_count: usize = map.values().sum();
    let median_pos = elem_count / 2;
    let mut sum = 0usize;
    let mut current_idx = 0usize;
    let mut median = 0usize;
    let mut max = 0usize;
    for (key, count) in map.iter() {
        if current_idx <= median_pos && current_idx + count > median_pos {
            median = *key;
        }
        sum += *key * *count;

        current_idx += count;
        if current_idx == elem_count {
            max = *key;
        }
    }
    let average = if elem_count > 0 {
        sum as f64 / elem_count as f64
    } else {
        0.0
    };

    CollectionStatistics::new(average, median, max)
}

#[derive(Debug, Default)]
pub struct ExecutionResultsStats {
    pub execution_results_size: BTreeMap<usize, usize>,
    pub chunk_count: BTreeMap<usize, usize>,
}

impl ExecutionResultsStats {
    pub fn feed(&mut self, execution_results: Vec<ExecutionResult>) -> Result<(), Error> {
        let bincode_encoded_execution_results_size = bincode::serialized_size(&execution_results)?;
        if let Some(count) = self
            .execution_results_size
            .get_mut(&(bincode_encoded_execution_results_size as usize))
        {
            *count += 1;
        } else {
            self.execution_results_size
                .insert(bincode_encoded_execution_results_size as usize, 1);
        }

        let bytesrepr_encoded_execution_results_length = execution_results.serialized_length();
        let chunks_in_execution_results =
            chunk_count_after_partition(bytesrepr_encoded_execution_results_length);
        if let Some(count) = self.chunk_count.get_mut(&chunks_in_execution_results) {
            *count += 1;
        } else {
            self.chunk_count.insert(chunks_in_execution_results, 1);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct CollectionStatistics {
    pub(crate) average: f64,
    pub(crate) median: usize,
    pub(crate) max: usize,
}

impl PartialEq for CollectionStatistics {
    fn eq(&self, other: &Self) -> bool {
        (self.average - other.average).abs() < FLOAT_TOLERANCE
            && self.median == other.median
            && self.max == other.max
    }
}

impl CollectionStatistics {
    pub(crate) fn new(average: f64, median: usize, max: usize) -> Self {
        Self {
            average,
            median,
            max,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ExecutionResultsSummary {
    pub(crate) execution_results_size: CollectionStatistics,
    pub(crate) chunks_statistics: CollectionStatistics,
}

impl From<ExecutionResultsStats> for ExecutionResultsSummary {
    fn from(stats: ExecutionResultsStats) -> Self {
        let execution_results_size = summarize_map(&stats.execution_results_size);
        let chunks_statistics = summarize_map(&stats.chunk_count);

        Self {
            execution_results_size,
            chunks_statistics,
        }
    }
}
