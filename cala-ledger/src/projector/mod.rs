//! Streaming projector for eventually-consistent (EC) account-set balances.
//!
//! Posters exclude EC sets from their inline fold-up; this module maintains
//! them by consuming the persistent outbox instead. A durable job drains the
//! ordered stream in bounded batches, folds `EntryCreated` events into each
//! account's EC ancestors, and commits the folded snapshots together with its
//! stream cursor in one transaction. The cursor advance is a compare-and-swap,
//! so a crashed, duplicated or restarted instance cannot apply a batch twice.

pub mod error;
mod repo;

use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

use es_entity::clock::ClockHandle;
use job::{
    CurrentJob, Job, JobCompletion, JobId, JobInitializer, JobRunner, JobSpawner, JobType,
    RetrySettings,
};
use obix::EventSequence;

use cala_types::{balance::BalanceSnapshot, entry::EntryValues};

use crate::{
    account_set::AccountSets,
    balance::{Balances, Snapshots},
    journal::Journals,
    ledger::CalaLedger,
    outbox::{ObixOutbox, OutboxEventPayload},
    primitives::{AccountId, AccountSetId, Currency, JournalId, TransactionId},
    transaction::{Transaction, Transactions},
};

use error::ProjectorError;
use repo::ProjectorRepo;

pub(crate) const BALANCE_PROJECTOR_JOB_TYPE: JobType = JobType::new("balance-projector");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BalanceProjectorConfig {
    pub batch_size: usize,
    pub idle_timeout: Duration,
}

impl Default for BalanceProjectorConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            idle_timeout: Duration::from_millis(100),
        }
    }
}

/// The projector's durable stream position: the sequence of the last outbox
/// event whose effect has been committed, persisted as the job's
/// `execution_state_json`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectorState {
    pub sequence: i64,
}

/// Registers the projector with a [`job::Jobs`] runtime owned by the
/// embedder; spawn it with `spawn_unique` so repeated startups reuse the
/// single durable job row.
/// [`CalaLedgerConfig::ec_balance_projector`](crate::CalaLedgerConfig) does
/// the same against a runtime cala hosts itself; do one or the other.
pub struct BalanceProjectorInit {
    projector: BalanceProjector,
}

impl BalanceProjectorInit {
    pub fn new(ledger: &CalaLedger) -> Self {
        Self {
            projector: BalanceProjector::from_ledger(ledger),
        }
    }
}

impl JobInitializer for BalanceProjectorInit {
    type Config = BalanceProjectorConfig;

    fn job_type(&self) -> JobType {
        BALANCE_PROJECTOR_JOB_TYPE.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings::repeat_indefinitely()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: BalanceProjectorConfig = job.config()?;
        Ok(Box::new(BalanceProjectorJobRunner {
            projector: self.projector.clone(),
            config,
        }))
    }
}

struct BalanceProjectorJobRunner {
    projector: BalanceProjector,
    config: BalanceProjectorConfig,
}

#[async_trait]
impl JobRunner for BalanceProjectorJobRunner {
    #[instrument(name = "cala_ledger.projector.run", skip_all)]
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut state = current_job
            .execution_state::<ProjectorState>()?
            .unwrap_or_default();
        let mut stream = self.projector.listen_from(&state);
        loop {
            let batch = tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(JobCompletion::RescheduleNow);
                }
                batch = collect_batch(&mut stream, &self.config, true) => batch,
            };
            let Some(new_state) = batch.next_state() else {
                return Ok(JobCompletion::RescheduleNow);
            };
            self.projector
                .persist_batch(*current_job.id(), &state, &new_state, &batch.entries)
                .await?;
            state = new_state;
        }
    }
}

#[derive(Clone)]
struct BalanceProjector {
    pool: sqlx::PgPool,
    clock: ClockHandle,
    outbox: ObixOutbox,
    account_sets: AccountSets,
    balances: Balances,
    journals: Journals,
    transactions: Transactions,
}

struct CollectedBatch {
    entries: Vec<EntryValues>,
    last_sequence: i64,
    events: usize,
}

impl CollectedBatch {
    fn next_state(&self) -> Option<ProjectorState> {
        (self.events > 0).then_some(ProjectorState {
            sequence: self.last_sequence,
        })
    }
}

/// Drain up to `batch_size` events, ending the batch at the first
/// `idle_timeout` gap. With `wait_for_first` the initial event is awaited
/// without a timeout; without it a caught-up stream yields an empty batch.
/// Only `EntryCreated` payloads carry balance deltas; every other variant
/// counts toward the cursor only.
async fn collect_batch(
    stream: &mut obix::out::PersistentOutboxListener<OutboxEventPayload>,
    config: &BalanceProjectorConfig,
    wait_for_first: bool,
) -> CollectedBatch {
    let mut batch = CollectedBatch {
        entries: Vec::new(),
        last_sequence: 0,
        events: 0,
    };
    while batch.events < config.batch_size {
        let next = if batch.events == 0 && wait_for_first {
            stream.next().await
        } else {
            match tokio::time::timeout(config.idle_timeout, stream.next()).await {
                Ok(next) => next,
                Err(_) => break,
            }
        };
        let Some(event) = next else { break };
        batch.events += 1;
        batch.last_sequence = u64::from(event.sequence) as i64;
        if let Some(OutboxEventPayload::EntryCreated { entry }) = &event.payload {
            batch.entries.push(entry.clone());
        }
    }
    batch
}

impl BalanceProjector {
    fn from_ledger(ledger: &CalaLedger) -> Self {
        Self {
            pool: ledger.pool().clone(),
            clock: ledger.clock().clone(),
            outbox: ledger.outbox().clone(),
            account_sets: ledger.account_sets().clone(),
            balances: ledger.balances().clone(),
            journals: ledger.journals().clone(),
            transactions: ledger.transactions().clone(),
        }
    }

    fn listen_from(
        &self,
        state: &ProjectorState,
    ) -> obix::out::PersistentOutboxListener<OutboxEventPayload> {
        self.outbox
            .listen_persisted(Some(EventSequence::from(state.sequence as u64)))
    }

    #[instrument(
        name = "cala_ledger.projector.persist_batch",
        skip(self, state, new_state, entries),
        fields(n_entries = entries.len())
    )]
    async fn persist_batch(
        &self,
        job_id: JobId,
        state: &ProjectorState,
        new_state: &ProjectorState,
        entries: &[EntryValues],
    ) -> Result<(), ProjectorError> {
        let mut op = es_entity::DbOp::init_with_clock(&self.pool, &self.clock)
            .await?
            .with_clock_time();
        self.fold_batch_in_op(&mut op, entries).await?;
        self.advance_cursor_in_op(&mut op, job_id, state.sequence, new_state)
            .await?;
        op.commit().await?;
        Ok(())
    }

    #[instrument(
        name = "cala_ledger.projector.fold_batch_in_op",
        skip_all,
        fields(n_entries = entries.len()),
        err(level = "warn")
    )]
    async fn fold_batch_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
        entries: &[EntryValues],
    ) -> Result<(), ProjectorError> {
        if entries.is_empty() {
            return Ok(());
        }
        let ec_set_ids = self.list_ec_set_ids_in_op(op).await?;
        let mut by_journal: Vec<(JournalId, Vec<&EntryValues>)> = Vec::new();
        for entry in entries {
            match by_journal
                .iter_mut()
                .find(|(id, _)| *id == entry.journal_id)
            {
                Some((_, journal_entries)) => journal_entries.push(entry),
                None => by_journal.push((entry.journal_id, vec![entry])),
            }
        }
        for (journal_id, journal_entries) in by_journal {
            self.fold_journal_batch_in_op(op, journal_id, &journal_entries, &ec_set_ids)
                .await?;
        }
        Ok(())
    }

    /// Fold one journal's slice of the batch, coalescing into one new
    /// snapshot per touched `(set, currency)`. Seeding from the current
    /// snapshot is a safe read-modify-write because the projector is the
    /// sole writer of EC set balances.
    async fn fold_journal_batch_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
        journal_id: JournalId,
        entries: &[&EntryValues],
        ec_set_ids: &HashSet<AccountSetId>,
    ) -> Result<(), ProjectorError> {
        let account_ids: Vec<AccountId> = entries
            .iter()
            .map(|entry| entry.account_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mappings = self
            .account_sets
            .fetch_mappings_in_op(op, journal_id, &account_ids)
            .await?;
        let mut ec_mappings: HashMap<AccountId, Vec<AccountSetId>> = HashMap::new();
        for (account_id, set_ids) in mappings {
            let ec_sets: Vec<AccountSetId> = set_ids
                .into_iter()
                .filter(|id| ec_set_ids.contains(id))
                .collect();
            if !ec_sets.is_empty() {
                ec_mappings.insert(account_id, ec_sets);
            }
        }
        if ec_mappings.is_empty() {
            return Ok(());
        }
        let set_account_ids: Vec<AccountId> = ec_mappings
            .values()
            .flatten()
            .map(AccountId::from)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut current_balances = self
            .balances
            .load_account_set_balances_batch(op, journal_id, &set_account_ids)
            .await?;
        let time = op.now();
        let empty = Vec::new();
        let mut running: BTreeMap<(AccountSetId, Currency), BalanceSnapshot> = BTreeMap::new();
        for entry in entries {
            for set_id in ec_mappings.get(&entry.account_id).unwrap_or(&empty) {
                let key = (*set_id, entry.currency);
                let snapshot = match running.remove(&key) {
                    Some(snapshot) => Snapshots::update_snapshot(time, snapshot, entry),
                    None => {
                        let set_account_id = AccountId::from(set_id);
                        match current_balances
                            .get_mut(&set_account_id)
                            .and_then(|balances| balances.remove(&entry.currency))
                        {
                            Some(current) => Snapshots::update_snapshot(time, current, entry),
                            None => Snapshots::new_snapshot(time, set_account_id, entry),
                        }
                    }
                };
                running.insert(key, snapshot);
            }
        }
        let new_snapshots: Vec<BalanceSnapshot> = running.into_values().collect();
        self.balances
            .insert_new_snapshots(op, journal_id, new_snapshots)
            .await?;
        self.rebuild_effective_in_op(op, journal_id, entries, &ec_mappings)
            .await?;
        Ok(())
    }

    /// The cumulative-by-date dimension is rebuilt rather than folded: each
    /// effective-enabled set the batch touched is rebuilt forward from its
    /// minimum batch effective date, inside the same operation.
    async fn rebuild_effective_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
        journal_id: JournalId,
        entries: &[&EntryValues],
        ec_mappings: &HashMap<AccountId, Vec<AccountSetId>>,
    ) -> Result<(), ProjectorError> {
        let journal = self.journals.find(journal_id).await?;
        if !journal.insert_effective_balances() {
            return Ok(());
        }
        let transaction_ids: Vec<TransactionId> = entries
            .iter()
            .map(|entry| entry.transaction_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let transactions: HashMap<TransactionId, Transaction> = self
            .transactions
            .find_all_in_op(op, &transaction_ids)
            .await?;
        let empty = Vec::new();
        let mut min_dates: BTreeMap<AccountSetId, chrono::NaiveDate> = BTreeMap::new();
        for entry in entries {
            let effective = transactions
                .get(&entry.transaction_id)
                .expect("transaction of a folded entry must exist")
                .values()
                .effective;
            for set_id in ec_mappings.get(&entry.account_id).unwrap_or(&empty) {
                min_dates
                    .entry(*set_id)
                    .and_modify(|min| {
                        if effective < *min {
                            *min = effective
                        }
                    })
                    .or_insert(effective);
            }
        }
        for (set_id, from_date) in min_dates {
            self.balances
                .rebuild_effective_balances_from_date_in_op(op, journal_id, &[set_id], from_date)
                .await?;
        }
        Ok(())
    }

    async fn list_ec_set_ids_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
    ) -> Result<HashSet<AccountSetId>, ProjectorError> {
        let mut ids = HashSet::new();
        let mut query = es_entity::PaginatedQueryArgs::default();
        loop {
            let ret = self
                .account_sets
                .list_eventually_consistent_ids_in_op(op, query)
                .await?;
            let has_next_page = ret.has_next_page;
            let end_cursor = ret.end_cursor;
            ids.extend(ret.entities);
            if !has_next_page {
                break;
            }
            query = es_entity::PaginatedQueryArgs {
                first: 100,
                after: end_cursor,
            };
        }
        Ok(ids)
    }

    #[instrument(
        name = "cala_ledger.projector.advance_cursor_in_op",
        skip(self, op, new_state),
        err(level = "warn")
    )]
    async fn advance_cursor_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
        job_id: JobId,
        expected: i64,
        new_state: &ProjectorState,
    ) -> Result<(), ProjectorError> {
        ProjectorRepo::advance_cursor_in_op(op, job_id, expected, new_state).await
    }

    async fn run_single_batch(
        &self,
        job_id: JobId,
        config: &BalanceProjectorConfig,
        state: &mut ProjectorState,
    ) -> Result<usize, ProjectorError> {
        let mut op = es_entity::DbOp::init_with_clock(&self.pool, &self.clock)
            .await?
            .with_clock_time();
        let events = self
            .run_single_batch_in_op(&mut op, job_id, config, state)
            .await?;
        if events == 0 {
            return Ok(0);
        }
        op.commit().await?;
        Ok(events)
    }

    async fn run_single_batch_in_op(
        &self,
        op: &mut es_entity::DbOpWithTime<'_>,
        job_id: JobId,
        config: &BalanceProjectorConfig,
        state: &mut ProjectorState,
    ) -> Result<usize, ProjectorError> {
        let mut stream = self.listen_from(state);
        let batch = collect_batch(&mut stream, config, false).await;
        let Some(new_state) = batch.next_state() else {
            return Ok(0);
        };
        self.fold_batch_in_op(op, &batch.entries).await?;
        self.advance_cursor_in_op(op, job_id, state.sequence, &new_state)
            .await?;
        *state = new_state;
        Ok(batch.events)
    }
}

/// Run one projector batch outside the job host: drain one bounded batch
/// from the cursor in `state`, fold it and advance the cursor, committed
/// atomically. Returns the number of outbox events consumed; `0` means
/// caught up. Exists for tests that need deterministic batch boundaries.
#[doc(hidden)]
pub async fn run_single_batch(
    ledger: &CalaLedger,
    job_id: JobId,
    config: &BalanceProjectorConfig,
    state: &mut ProjectorState,
) -> Result<usize, ProjectorError> {
    BalanceProjector::from_ledger(ledger)
        .run_single_batch(job_id, config, state)
        .await
}

/// [`run_single_batch`], composed into a caller-owned operation; `state` is
/// only meaningful if the caller commits the operation.
#[doc(hidden)]
pub async fn run_single_batch_in_op(
    ledger: &CalaLedger,
    op: &mut es_entity::DbOpWithTime<'_>,
    job_id: JobId,
    config: &BalanceProjectorConfig,
    state: &mut ProjectorState,
) -> Result<usize, ProjectorError> {
    BalanceProjector::from_ledger(ledger)
        .run_single_batch_in_op(op, job_id, config, state)
        .await
}
