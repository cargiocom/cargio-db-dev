use std::result::Result;

use log::warn;

const STEPS: usize = 20;
const PROGRESS_MULTIPLIER: u64 = 100 / STEPS as u64;
const NULL_TOTAL_TO_PROCESS_ERROR: &str = "Cannot initialize total to process with 0";

pub struct ProgressTracker {
    total_to_process: usize,
    processed: usize,
    progress_factor: u64,
    log_progress: Box<dyn Fn(u64)>,
}

impl ProgressTracker {
    pub fn new(
        total_to_process: usize,
        log_progress: Box<dyn Fn(u64)>,
    ) -> Result<Self, &'static str> {
        if total_to_process == 0 {
            Err(NULL_TOTAL_TO_PROCESS_ERROR)
        } else {
            Ok(Self {
                total_to_process,
                processed: 0,
                progress_factor: 1,
                log_progress,
            })
        }
    }

    pub fn advance_by(&mut self, step: usize) {
        self.processed += step;
        while self.processed * STEPS >= self.total_to_process * self.progress_factor as usize {
            (*self.log_progress)(self.progress_factor * PROGRESS_MULTIPLIER);
            self.progress_factor += 1;
        }
        if self.processed > self.total_to_process {
            warn!(
                "Exceeded total amount to process {} by {}",
                self.total_to_process,
                self.processed - self.total_to_process
            );
        }
    }
}
