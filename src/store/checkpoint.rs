use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::types::{Error, Result};

use super::{CandyStore, CheckpointFailure, StoreInner};

/// RAII guard that marks the checkpoint worker as shut down and wakes all
/// waiters when the worker thread exits — whether it returns normally or
/// panics.  This prevents `wait_for_checkpoint_epoch` from blocking forever
/// if the worker encounters an unexpected panic.
struct WorkerShutdownGuard<'a> {
    inner: &'a StoreInner,
}

impl Drop for WorkerShutdownGuard<'_> {
    fn drop(&mut self) {
        let _state = self.inner.checkpoint_state.lock();
        self.inner
            .checkpoint_shutting_down
            .store(true, Ordering::Release);
        self.inner.checkpoint_condvar.notify_all();
    }
}

impl StoreInner {
    fn request_checkpoint_epoch_locked(state: &mut super::CheckpointState) -> u64 {
        state.requested_epoch = state
            .requested_epoch
            .checked_add(1)
            .expect("checkpoint epoch overflow");
        state.requested_epoch
    }

    pub(super) fn approx_uncheckpointed_bytes(&self) -> u64 {
        let active_idx = self.active_file_idx.load(Ordering::Acquire);
        let files = self.data_files.read();
        let Some(active_file) = files.get(&active_idx) else {
            return 0;
        };

        let (commit_file_ordinal, commit_offset) = self.index_file.checkpoint_cursor();
        let commit_offset = if commit_file_ordinal == active_file.file_ordinal {
            commit_offset
        } else {
            0
        };

        active_file.used_bytes().saturating_sub(commit_offset)
    }

    pub(super) fn request_checkpoint_epoch(&self) -> u64 {
        let mut state = self.checkpoint_state.lock();
        let target_epoch = Self::request_checkpoint_epoch_locked(&mut state);
        self.checkpoint_condvar.notify_all();
        target_epoch
    }

    pub(super) fn note_checkpoint_write(&self, end_offset: u64) {
        let Some(threshold) = self.config.checkpoint_delta_bytes else {
            return;
        };

        // this is not atomic but it's fine -- rotation triggers checkpointing so if the last checkpoint
        // happened before this file, we know a checkpoint is in progress. skip.
        let (ordinal, commit_offset) = self.index_file.checkpoint_cursor();
        if self.active_file_ordinal.load(Ordering::Relaxed) != ordinal {
            return;
        }

        if end_offset <= commit_offset + threshold as u64 {
            return;
        }

        let mut state = self.checkpoint_state.lock();
        if state.requested_epoch > state.completed_epoch {
            return;
        }

        Self::request_checkpoint_epoch_locked(&mut state);
        self.checkpoint_condvar.notify_all();
    }

    pub(super) fn wait_for_checkpoint_epoch(&self, target_epoch: u64) -> Result<()> {
        let mut state = self.checkpoint_state.lock();
        loop {
            if state.completed_epoch >= target_epoch {
                return Ok(());
            }
            if state.handled_epoch >= target_epoch && state.last_failure_epoch >= target_epoch {
                return Err(state
                    .last_failure
                    .as_ref()
                    .map(CheckpointFailure::to_error)
                    .unwrap_or_else(|| {
                        Error::CheckpointShutdown(
                            "checkpoint worker stopped before completing request".into(),
                        )
                    }));
            }
            if self.checkpoint_shutting_down.load(Ordering::Acquire) {
                return Err(Error::CheckpointShutdown(
                    "checkpoint worker is shutting down".into(),
                ));
            }
            self.checkpoint_condvar.wait(&mut state);
        }
    }

    fn run_checkpoint_worker(self: &Arc<Self>) {
        let _shutdown_guard = WorkerShutdownGuard { inner: self };
        let interval = self.config.checkpoint_interval;
        let threshold = self.config.checkpoint_delta_bytes.map(|value| value as u64);
        let mut last_checkpoint_at = Instant::now();

        loop {
            let mut interval_elapsed = false;
            {
                let mut state = self.checkpoint_state.lock();
                loop {
                    if self.checkpoint_shutting_down.load(Ordering::Acquire) {
                        self.checkpoint_condvar.notify_all();
                        return;
                    }

                    if state.handled_epoch < state.requested_epoch {
                        break;
                    }

                    if let Some(interval) = interval {
                        let remaining = interval.saturating_sub(last_checkpoint_at.elapsed());
                        if remaining.is_zero() {
                            interval_elapsed = true;
                            break;
                        }
                        let wait_result = self.checkpoint_condvar.wait_for(&mut state, remaining);
                        if wait_result.timed_out() {
                            interval_elapsed = true;
                            break;
                        }
                    } else {
                        self.checkpoint_condvar.wait(&mut state);
                    }
                }
            }

            // Acquire all logical locks so compound list/queue operations
            // are quiesced, then snapshot the checkpoint progress.  Release
            // the logical locks *before* the expensive fsync phase so writers
            // are only blocked for the snapshot, not for the I/O.
            let (target_epoch, snapshot) = {
                let _logical_guards = self
                    .list_meta_locks
                    .iter()
                    .map(|lock| lock.write())
                    .collect::<Vec<_>>();
                let mut state = self.checkpoint_state.lock();
                if self.checkpoint_shutting_down.load(Ordering::Acquire) {
                    self.checkpoint_condvar.notify_all();
                    return;
                }

                let target_epoch =
                    (state.handled_epoch < state.requested_epoch).then_some(state.requested_epoch);
                if target_epoch.is_none() && !interval_elapsed {
                    continue;
                }

                let current_cursor = self.index_file.checkpoint_cursor();
                let snapshot = match self.snapshot_checkpoint_progress() {
                    Ok(snapshot) => {
                        let snapshot_is_noop = snapshot.checkpoint_ordinal == current_cursor.0
                            && snapshot.checkpoint_offset == current_cursor.1
                            && snapshot.checkpointed_delta == 0;
                        if snapshot_is_noop {
                            if let Some(target_epoch) = target_epoch {
                                state.handled_epoch = target_epoch;
                                state.completed_epoch = target_epoch;
                                state.last_checkpoint_dur_ms = 0;
                                if state.last_failure_epoch <= state.completed_epoch {
                                    state.last_failure_epoch = 0;
                                    state.last_failure = None;
                                }
                                self.checkpoint_condvar.notify_all();
                            }
                            last_checkpoint_at = Instant::now();
                            continue;
                        }
                        Ok(snapshot)
                    }
                    Err(e) => Err(e),
                };

                drop(state);
                // _logical_guards dropped here
                (target_epoch, snapshot)
            };

            let started_at = Instant::now();
            let snapshot_for_follow_up = snapshot.as_ref().ok().copied();
            let result = snapshot.and_then(|snap| self.sync_checkpoint(snap));

            let mut state = self.checkpoint_state.lock();
            match result {
                Ok(()) => {
                    state.last_checkpoint_dur_ms =
                        u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                    if let Some(target_epoch) = target_epoch {
                        state.handled_epoch = state.handled_epoch.max(target_epoch);
                        state.completed_epoch = state.completed_epoch.max(target_epoch);
                    }
                    if state.last_failure_epoch <= state.completed_epoch {
                        state.last_failure_epoch = 0;
                        state.last_failure = None;
                    }

                    let should_request_follow_up = match (threshold, snapshot_for_follow_up) {
                        (Some(threshold), Some(snapshot)) => {
                            let active_idx = self.active_file_idx.load(Ordering::Acquire);
                            let files = self.data_files.read();
                            match files.get(&active_idx) {
                                Some(active_file) => {
                                    active_file.file_ordinal == snapshot.checkpoint_ordinal
                                        && active_file
                                            .used_bytes()
                                            .saturating_sub(snapshot.checkpoint_offset)
                                            >= threshold
                                }
                                None => false,
                            }
                        }
                        _ => false,
                    };
                    if should_request_follow_up && state.handled_epoch >= state.requested_epoch {
                        Self::request_checkpoint_epoch_locked(&mut state);
                    }
                }
                Err(err) => {
                    self.stats.checkpoint_errors.fetch_add(1, Ordering::Relaxed);
                    // For interval-only failures (target_epoch is None), allocate
                    // a synthetic epoch so last_failure_epoch is set and the error
                    // is observable rather than silently cleared.
                    let failure_epoch = target_epoch
                        .unwrap_or_else(|| Self::request_checkpoint_epoch_locked(&mut state));
                    state.handled_epoch = state.handled_epoch.max(failure_epoch);
                    state.last_failure_epoch = failure_epoch;
                    state.last_failure = Some(CheckpointFailure::from_error(err));
                }
            }
            self.checkpoint_condvar.notify_all();
            last_checkpoint_at = Instant::now();
        }
    }
}

impl CandyStore {
    pub(super) fn start_checkpoint_worker(&self) {
        let mut checkpoint_thd = self.checkpoint_thd.lock();
        if checkpoint_thd.is_some() {
            return;
        }

        self.inner
            .checkpoint_shutting_down
            .store(false, Ordering::Release);
        let ctx = Arc::clone(&self.inner);
        let thd = std::thread::Builder::new()
            .name("candy_checkpoint".into())
            .spawn(move || {
                ctx.run_checkpoint_worker();
            })
            .unwrap();
        *checkpoint_thd = Some(thd);
    }

    pub(super) fn stop_checkpoint_worker(&self) {
        {
            let _state = self.inner.checkpoint_state.lock();
            self.inner
                .checkpoint_shutting_down
                .store(true, Ordering::Release);
            self.inner.checkpoint_condvar.notify_all();
        }
        if let Some(thd) = self.checkpoint_thd.lock().take() {
            let _ = thd.join();
        }
    }
}
