//! The outbox, jobs and workers, approvals, scoped fetches, parent worker
//! controls, diagnostics, links, the weekly archives and the configuration
//! journal: the store's modules behind the storage contract.
use super::Sqlite;
use crate::{
    approvals, archive, configuration, diagnostics, fetch, links, outbox, work, worker_controls,
};
use anyhow::{Context, Result};
use fridica_core::{
    config::{registry::Registry, Limits},
    delivery::{ClaimedPost, DeliveryOutcome, Post},
    parent::{ParentRequest, WorkerControl},
    store::{
        ApprovalSettlement, ApprovalStart, Approvals, Archive, ArchiveHit, ClaimedJob, Completed,
        Completion, ConfigurationIntents, Diagnostics, Fetches, Jobs, Links, NewApproval, Outbox,
        PendingConfigurationEdit, PendingWorkerControl, PostOutcome, PreviousSnapshot,
        WorkSnapshot, WorkerControls,
    },
    worker::{Approval, Job, WorkerRecord},
    Authority,
};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};

impl Outbox for Sqlite<'_> {
    fn queue_post(&mut self, post: &Post, now: f64) -> Result<i64> {
        outbox::enqueue_tx(self.0, post, now)
    }
    fn ready_posts(&mut self, now: f64, limit: usize) -> Result<Vec<i64>> {
        outbox::ready_tx(self.0, now, limit)
    }
    fn claim_post(&mut self, now: f64, id: Option<i64>) -> Result<Option<ClaimedPost>> {
        outbox::claim_tx(self.0, now, id)
    }
    fn finish_delivery(
        &mut self,
        claim: &ClaimedPost,
        outcome: &DeliveryOutcome,
        owner: &str,
        now: f64,
    ) -> Result<PostOutcome> {
        outbox::complete_tx(self.0, claim, outcome, owner, now)
    }
    fn confirm_post(&mut self, id: i64, reference: &str, now: f64) -> Result<()> {
        outbox::confirm_tx(self.0, id, reference, now)
    }
    fn recover_posts(&mut self, now: f64) -> Result<usize> {
        outbox::recover_tx(self.0, now)
    }
    fn retry_post(&mut self, id: i64, actor: Authority, now: f64) -> Result<bool> {
        outbox::requeue_tx(self.0, id, actor, now)
    }
}

impl Jobs for Sqlite<'_> {
    fn job_record(&mut self, id: &str) -> Result<Job> {
        work::job(self.0, id)
    }
    fn worker_record(&mut self, id: &str) -> Result<WorkerRecord> {
        work::worker(self.0, id)
    }
    fn add_workers(&mut self, workers: &[WorkerRecord], now: f64) -> Result<()> {
        for worker in workers {
            work::add_worker_tx(self.0, worker, now)?;
        }
        Ok(())
    }
    fn queue_jobs(&mut self, jobs: &[Job], now: f64) -> Result<()> {
        for job in jobs {
            work::enqueue_tx(self.0, job, now)?;
        }
        Ok(())
    }
    fn work_context(&mut self, session: &str) -> Result<Value> {
        work::context_tx(self.0, session)
    }
    fn previous_snapshot(&mut self, worker: &str, job: &str) -> Result<Option<PreviousSnapshot>> {
        work::previous_snapshot_tx(self.0, worker, job)
    }
    fn delegable_files(&mut self, session: &str) -> Result<Vec<Value>> {
        work::files_tx(self.0, session)
    }
    fn work_snapshot(&mut self) -> Result<WorkSnapshot> {
        work::snapshot_tx(self.0)
    }
    fn claim_job(
        &mut self,
        id: &str,
        slot: usize,
        machines: &Registry,
        limits: &Limits,
        now: f64,
    ) -> Result<Option<ClaimedJob>> {
        work::claim_tx(self.0, id, slot, machines, limits, now)
    }
    fn record_job_progress(
        &mut self,
        job: &str,
        attempt: u32,
        text: &str,
        now: f64,
    ) -> Result<bool> {
        work::progress_tx(self.0, job, attempt, text, now)
    }
    fn complete_job(
        &mut self,
        id: &str,
        attempt: u32,
        completion: &Completion,
        now: f64,
    ) -> Result<Completed> {
        work::complete_tx(self.0, id, attempt, completion, now)
    }
    fn stop_worker(&mut self, worker: &str, actor: &str, now: f64) -> Result<()> {
        work::stop_tx(self.0, worker, actor, now)
    }
    fn recover_jobs(&mut self, now: f64) -> Result<usize> {
        work::recover_tx(self.0, now)
    }
    fn busy_by_machine(&mut self) -> Result<BTreeMap<String, i64>> {
        work::busy_by_machine_tx(self.0)
    }
}

impl Approvals for Sqlite<'_> {
    fn approval(&mut self, id: &str) -> Result<Option<Approval>> {
        approvals::get_tx(self.0, id)
    }
    fn pending_approvals(&mut self, limit: usize) -> Result<Vec<Approval>> {
        approvals::pending_tx(self.0, limit)
    }
    fn begin_approval(&mut self, request: &NewApproval) -> Result<ApprovalStart> {
        approvals::begin_tx(self.0, request)
    }
    fn settle_approval(
        &mut self,
        id: &str,
        settlement: ApprovalSettlement,
        actor: &str,
        now: f64,
    ) -> Result<bool> {
        approvals::settle_tx(self.0, id, settlement, actor, now)
    }
    fn cancel_worker_approvals(&mut self, worker: &str, now: f64) -> Result<()> {
        approvals::cancel_worker_tx(self.0, worker, now)
    }
    fn cancel_pending_approvals(&mut self, now: f64) -> Result<()> {
        approvals::cancel_all_tx(self.0, now)
    }
}

impl Fetches for Sqlite<'_> {
    fn begin_fetch(&mut self, job: &Job, request: &Value, now: f64) -> Result<Option<i64>> {
        fetch::begin_tx(self.0, job, request, now)
    }
    fn finish_fetch(&mut self, job: &Job, seq: i64, result: &Value, now: f64) -> Result<bool> {
        fetch::finish_tx(self.0, job, seq, result, now)
    }
}

impl WorkerControls for Sqlite<'_> {
    fn controlled_jobs(&mut self, session: &str) -> Result<Vec<Value>> {
        worker_controls::jobs_tx(self.0, session)
    }
    fn worker_control_pending(&mut self, session: &str) -> Result<bool> {
        worker_controls::pending_tx(self.0, session)
    }
    fn recent_worker_controls(&mut self, session: &str) -> Result<Vec<Value>> {
        worker_controls::recent_tx(self.0, session)
    }
    fn interrupt_pending(&mut self, job: &str, attempt: u32) -> Result<bool> {
        worker_controls::interrupt_pending_tx(self.0, job, attempt)
    }
    fn worker_controls_current(
        &mut self,
        request: &ParentRequest,
        controls: &[WorkerControl],
    ) -> Result<bool> {
        worker_controls::current_tx(self.0, request, controls)
    }
    fn queue_worker_controls(
        &mut self,
        request: &ParentRequest,
        controls: &[WorkerControl],
        now: f64,
    ) -> Result<()> {
        worker_controls::enqueue_tx(self.0, request, controls, now)
    }
    fn pending_worker_controls(&mut self) -> Result<Vec<PendingWorkerControl>> {
        worker_controls::pending_intents_tx(self.0)
    }
    fn complete_worker_control(&mut self, seq: i64, outcome: &str, now: f64) -> Result<()> {
        worker_controls::complete_tx(self.0, seq, outcome, now)
    }
}

impl Diagnostics for Sqlite<'_> {
    fn slack_scopes(&mut self) -> Result<Option<String>> {
        diagnostics::slack_scopes_tx(self.0)
    }
}

impl Links for Sqlite<'_> {
    fn record_links(
        &mut self,
        workspace: &str,
        channel: &str,
        session: &str,
        text: &str,
        now: f64,
    ) -> Result<()> {
        links::record_tx(self.0, workspace, channel, session, text, now)
    }
    fn backfill_links(&mut self, now: f64, window: f64) -> Result<usize> {
        links::backfill_tx(self.0, now, window)
    }
}

impl Archive for Sqlite<'_> {
    fn revive_thread(&mut self, session: &str, now: f64) -> Result<bool> {
        archive::revive_tx(self.0, session, now)
    }
    fn revive_or_note(&mut self, session: &str, now: f64) -> Result<()> {
        archive::revive_or_note(self.0, session, now)
    }
    fn search_archives(&mut self, query: &str, limit: usize) -> Result<Vec<ArchiveHit>> {
        // The archives sit beside the database file.
        let db = self
            .0
            .path()
            .filter(|p| !p.is_empty())
            .context("the store has no database file")?;
        archive::search(Path::new(db), query, limit)
    }
}

impl ConfigurationIntents for Sqlite<'_> {
    fn pending_configuration_edit(&mut self) -> Result<Option<PendingConfigurationEdit>> {
        configuration::pending_tx(self.0)
    }
    fn complete_configuration_edit(&mut self, seq: i64, applied: bool, now: f64) -> Result<()> {
        configuration::complete_tx(self.0, seq, applied, now)
    }
}
