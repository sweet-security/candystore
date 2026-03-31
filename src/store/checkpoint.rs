use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::types::{Error, Result};

use super::{CandyStore, CheckpointFailure, CheckpointSnapshot, StoreInner};

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

enum CheckpointRun {
    Shutdown,
    Idle {
        reset_timer: bool,
    },
    Ready {
        target_epoch: Option<u64>,
        snapshot: Result<CheckpointSnapshot>,
    },
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

    fn wait_for_checkpoint_trigger(
        &self,
        interval: Option<Duration>,
        last_checkpoint_at: Instant,
    ) -> Option<bool> {
        let mut state = self.checkpoint_state.lock();
        loop {
            if self.checkpoint_shutting_down.load(Ordering::Acquire) {
                self.checkpoint_condvar.notify_all();
                return None;
            }

            if state.handled_epoch < state.requested_epoch {
                return Some(false);
            }

            if let Some(interval) = interval {
                let remaining = interval.saturating_sub(last_checkpoint_at.elapsed());
                if remaining.is_zero() {
                    return Some(true);
                }
                let wait_result = self.checkpoint_condvar.wait_for(&mut state, remaining);
                if wait_result.timed_out() {
                    return Some(true);
                }
            } else {
                self.checkpoint_condvar.wait(&mut state);
            }
        }
    }

    fn complete_checkpoint_noop(
        &self,
        state: &mut super::CheckpointState,
        target_epoch: Option<u64>,
    ) {
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
    }

    fn prepare_checkpoint_run(&self, interval_elapsed: bool) -> CheckpointRun {
        let _logical_guards = self
            .list_meta_locks
            .iter()
            .map(|lock| lock.write())
            .collect::<Vec<_>>();
        let mut state = self.checkpoint_state.lock();
        if self.checkpoint_shutting_down.load(Ordering::Acquire) {
            self.checkpoint_condvar.notify_all();
            return CheckpointRun::Shutdown;
        }

        let target_epoch =
            (state.handled_epoch < state.requested_epoch).then_some(state.requested_epoch);
        if target_epoch.is_none() && !interval_elapsed {
            return CheckpointRun::Idle { reset_timer: false };
        }

        let current_cursor = self.index_file.checkpoint_cursor();
        match self.snapshot_checkpoint_progress() {
            Ok(snapshot)
                if snapshot.checkpoint_ordinal == current_cursor.0
                    && snapshot.checkpoint_offset == current_cursor.1
                    && snapshot.checkpointed_delta == 0 =>
            {
                self.complete_checkpoint_noop(&mut state, target_epoch);
                CheckpointRun::Idle { reset_timer: true }
            }
            Ok(snapshot) => CheckpointRun::Ready {
                target_epoch,
                snapshot: Ok(snapshot),
            },
            Err(err) => CheckpointRun::Ready {
                target_epoch,
                snapshot: Err(err),
            },
        }
    }

    fn checkpoint_needs_follow_up(&self, threshold: u64, snapshot: CheckpointSnapshot) -> bool {
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

    fn finish_checkpoint_run(
        &self,
        target_epoch: Option<u64>,
        snapshot: Result<CheckpointSnapshot>,
        threshold: Option<u64>,
        started_at: Instant,
    ) {
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
                        self.checkpoint_needs_follow_up(threshold, snapshot)
                    }
                    _ => false,
                };
                if should_request_follow_up && state.handled_epoch >= state.requested_epoch {
                    Self::request_checkpoint_epoch_locked(&mut state);
                }
            }
            Err(err) => {
                self.stats.checkpoint_errors.fetch_add(1, Ordering::Relaxed);
                let failure_epoch = target_epoch
                    .unwrap_or_else(|| Self::request_checkpoint_epoch_locked(&mut state));
                state.handled_epoch = state.handled_epoch.max(failure_epoch);
                state.last_failure_epoch = failure_epoch;
                state.last_failure = Some(CheckpointFailure::from_error(err));
            }
        }
        self.checkpoint_condvar.notify_all();
    }

    fn run_checkpoint_worker(self: &Arc<Self>) {
        let _shutdown_guard = WorkerShutdownGuard { inner: self };
        let interval = self.config.checkpoint_interval;
        let threshold = self.config.checkpoint_delta_bytes.map(|value| value as u64);
        let mut last_checkpoint_at = Instant::now();

        loop {
            let Some(interval_elapsed) =
                self.wait_for_checkpoint_trigger(interval, last_checkpoint_at)
            else {
                return;
            };

            let (target_epoch, snapshot) = match self.prepare_checkpoint_run(interval_elapsed) {
                CheckpointRun::Shutdown => return,
                CheckpointRun::Idle { reset_timer } => {
                    if reset_timer {
                        last_checkpoint_at = Instant::now();
                    }
                    continue;
                }
                CheckpointRun::Ready {
                    target_epoch,
                    snapshot,
                } => (target_epoch, snapshot),
            };

            let started_at = Instant::now();
            self.finish_checkpoint_run(target_epoch, snapshot, threshold, started_at);
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
