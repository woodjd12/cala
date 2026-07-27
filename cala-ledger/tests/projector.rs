mod helpers;

use rand::distr::{Alphanumeric, SampleString};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use std::time::Duration;

use cala_ledger::{
    account::*,
    account_set::*,
    projector::{self, error::ProjectorError, BalanceProjectorConfig, ProjectorState},
    tx_template::*,
    *,
};
use job::JobId;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

/// A ledger that hosts no projector; the test drives batches itself via
/// `projector::run_single_batch` and controls where each batch boundary
/// falls.
async fn direct_drive_ledger(pool: &sqlx::PgPool) -> anyhow::Result<CalaLedger> {
    let config = CalaLedgerConfig::builder()
        .pool(pool.clone())
        .exec_migrations(false)
        .build()?;
    Ok(CalaLedger::init(config).await?)
}

/// Batch window sized far above anything concurrent suite activity can
/// produce, so a batch never splits while backfilling committed events.
fn direct_config() -> BalanceProjectorConfig {
    BalanceProjectorConfig {
        batch_size: 10_000,
        idle_timeout: Duration::from_millis(1_500),
    }
}

/// Seed a throwaway durable job row for a direct-runner test: the CAS
/// needs a `job_executions` row carrying the test's start cursor.
async fn seed_projector_job(pool: &sqlx::PgPool, state: &ProjectorState) -> anyhow::Result<JobId> {
    let job_id = JobId::new();
    sqlx::query(
        "INSERT INTO jobs (id, unique_per_type, job_type) \
         VALUES ($1, FALSE, 'balance-projector-test')",
    )
    .bind(uuid::Uuid::from(job_id))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO job_executions (id, job_type, execution_state_json, alive_at, created_at) \
         VALUES ($1, 'balance-projector-test', $2, NOW(), NOW())",
    )
    .bind(uuid::Uuid::from(job_id))
    .bind(serde_json::to_value(state)?)
    .execute(pool)
    .await?;
    Ok(job_id)
}

async fn history_row_count(
    pool: &sqlx::PgPool,
    journal_id: JournalId,
    account_id: AccountId,
    currency: Currency,
) -> anyhow::Result<i64> {
    use sqlx::Row as _;
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS cnt FROM cala_balance_history \
         WHERE journal_id = $1 AND account_id = $2 AND currency = $3",
    )
    .bind(journal_id)
    .bind(account_id)
    .bind(currency.code())
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("cnt")?)
}

struct EcFixture {
    cala: CalaLedger,
    journal_id: JournalId,
    tx_code: String,
    sender: AccountId,
    leaf: AccountId,
    inline_set: AccountSetId,
    ec_set: AccountSetId,
}

impl EcFixture {
    async fn init(pool: &sqlx::PgPool, effective_balances: bool) -> anyhow::Result<Self> {
        let cala = direct_drive_ledger(pool).await?;
        let journal = if effective_balances {
            cala.journals()
                .create(helpers::test_journal_with_effective_balances())
                .await?
        } else {
            cala.journals().create(helpers::test_journal()).await?
        };
        let (sender, leaf) = helpers::test_accounts();
        let sender = cala.accounts().create(sender).await?;
        let leaf = cala.accounts().create(leaf).await?;
        let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
        cala.tx_templates()
            .create(helpers::currency_conversion_template(&tx_code))
            .await?;
        let inline_set = cala
            .account_sets()
            .create(new_inline_set(journal.id(), "Inline Reference Set"))
            .await?;
        let ec_set = cala
            .account_sets()
            .create(new_ec_set(journal.id(), "EC Set"))
            .await?;
        cala.account_sets()
            .add_member(inline_set.id(), leaf.id())
            .await?;
        cala.account_sets()
            .add_member(ec_set.id(), leaf.id())
            .await?;
        Ok(Self {
            journal_id: journal.id(),
            tx_code,
            sender: sender.id(),
            leaf: leaf.id(),
            inline_set: inline_set.id(),
            ec_set: ec_set.id(),
            cala,
        })
    }

    async fn post(&self) -> anyhow::Result<()> {
        post_conversion(
            &self.cala,
            self.journal_id,
            &self.tx_code,
            self.sender,
            self.leaf,
        )
        .await
    }

    async fn post_effective(&self, effective: chrono::NaiveDate) -> anyhow::Result<()> {
        let mut params = Params::new();
        params.insert("journal_id", self.journal_id.to_string());
        params.insert("sender", self.sender);
        params.insert("recipient", self.leaf);
        params.insert("effective", effective);
        self.cala
            .post_transaction(TransactionId::new(), &self.tx_code, params)
            .await?;
        Ok(())
    }
}

fn new_ec_set(journal_id: JournalId, name: &str) -> NewAccountSet {
    NewAccountSet::builder()
        .id(AccountSetId::new())
        .name(name)
        .journal_id(journal_id)
        .balance_rollup(BalanceRollup::EventuallyConsistent)
        .build()
        .unwrap()
}

fn new_inline_set(journal_id: JournalId, name: &str) -> NewAccountSet {
    NewAccountSet::builder()
        .id(AccountSetId::new())
        .name(name)
        .journal_id(journal_id)
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap()
}

async fn post_conversion(
    cala: &CalaLedger,
    journal_id: JournalId,
    tx_code: &str,
    sender: AccountId,
    recipient: AccountId,
) -> anyhow::Result<()> {
    let mut params = Params::new();
    params.insert("journal_id", journal_id.to_string());
    params.insert("sender", sender);
    params.insert("recipient", recipient);
    cala.post_transaction(TransactionId::new(), tx_code, params)
        .await?;
    Ok(())
}

/// With the flag off, posting to an EC member must leave the set unfolded;
/// with it on, the same posting converges to the inline reference.
#[tokio::test]
async fn projector_flag_gates_folding() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;

    let cala_off = helpers::init_cala_with_projector(pool.clone(), false).await?;
    let journal = cala_off.journals().create(helpers::test_journal()).await?;
    let (sender, leaf_off) = helpers::test_accounts();
    let sender = cala_off.accounts().create(sender).await?;
    let leaf_off = cala_off.accounts().create(leaf_off).await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala_off
        .tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;
    let ec_off = cala_off
        .account_sets()
        .create(new_ec_set(journal.id(), "Flag Off EC Set"))
        .await?;
    cala_off
        .account_sets()
        .add_member(ec_off.id(), leaf_off.id())
        .await?;
    post_conversion(
        &cala_off,
        journal.id(),
        &tx_code,
        sender.id(),
        leaf_off.id(),
    )
    .await?;

    cala_off
        .balances()
        .find(journal.id(), leaf_off.id(), btc)
        .await?;
    assert!(
        cala_off
            .balances()
            .find(journal.id(), ec_off.id(), btc)
            .await
            .is_err(),
        "with the flag off nothing may fold the EC set"
    );

    let cala_on = helpers::init_cala_with_projector(pool.clone(), true).await?;
    let (_, leaf_on) = helpers::test_accounts();
    let leaf_on = cala_on.accounts().create(leaf_on).await?;
    let inline_on = cala_on
        .account_sets()
        .create(new_inline_set(journal.id(), "Flag On Inline Set"))
        .await?;
    let ec_on = cala_on
        .account_sets()
        .create(new_ec_set(journal.id(), "Flag On EC Set"))
        .await?;
    cala_on
        .account_sets()
        .add_member(inline_on.id(), leaf_on.id())
        .await?;
    cala_on
        .account_sets()
        .add_member(ec_on.id(), leaf_on.id())
        .await?;
    for _ in 0..2 {
        post_conversion(&cala_on, journal.id(), &tx_code, sender.id(), leaf_on.id()).await?;
    }

    let inline_bal = cala_on
        .balances()
        .find(journal.id(), inline_on.id(), btc)
        .await?;
    let ec_bal = helpers::wait_for_ec_convergence(
        &cala_on,
        journal.id(),
        ec_on.id(),
        btc,
        inline_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    assert_eq!(ec_bal.settled(), inline_bal.settled());

    cala_on.shutdown().await?;
    cala_on.shutdown().await?;

    Ok(())
}

/// Pre-advance the persisted cursor so the CAS predicate no longer
/// matches, then run one batch: the 0-row CAS must abort the entire
/// operation, snapshots included.
#[tokio::test]
async fn stale_cursor_cas_aborts_whole_batch() -> anyhow::Result<()> {
    use sqlx::Row as _;

    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let fixture = EcFixture::init(&pool, false).await?;

    let head = helpers::outbox_head(&pool).await?;
    let mut state = ProjectorState { sequence: head };
    let job_id = seed_projector_job(&pool, &state).await?;

    for _ in 0..2 {
        fixture.post().await?;
    }

    let fenced_sequence = head + 1_000_000;
    sqlx::query("UPDATE job_executions SET execution_state_json = $1 WHERE id = $2")
        .bind(serde_json::json!({ "sequence": fenced_sequence }))
        .bind(uuid::Uuid::from(job_id))
        .execute(&pool)
        .await?;

    let err = projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state)
        .await
        .expect_err("a stale cursor must fence the runner");
    assert!(
        matches!(err, ProjectorError::CursorFenced { .. }),
        "expected CursorFenced, got {err}"
    );

    assert_eq!(
        state.sequence, head,
        "a fenced batch must not move the cursor"
    );
    assert!(
        fixture
            .cala
            .balances()
            .find(fixture.journal_id, fixture.ec_set, btc)
            .await
            .is_err(),
        "the fenced batch's fold must roll back with the cursor"
    );
    let row = sqlx::query("SELECT execution_state_json FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(job_id))
        .fetch_one(&pool)
        .await?;
    let persisted: serde_json::Value = row.try_get("execution_state_json")?;
    assert_eq!(persisted["sequence"].as_i64(), Some(fenced_sequence));

    Ok(())
}

/// Two runner handles race the same window against one durable cursor:
/// exactly one may commit, and the folded outcome is identical to a
/// single run.
#[tokio::test]
async fn concurrent_projectors_fold_exactly_once() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let fixture = EcFixture::init(&pool, false).await?;

    let head = helpers::outbox_head(&pool).await?;
    let seed = ProjectorState { sequence: head };
    let job_id = seed_projector_job(&pool, &seed).await?;

    for _ in 0..3 {
        fixture.post().await?;
    }

    let config = direct_config();
    let mut state_a = seed;
    let mut state_b = seed;
    let (result_a, result_b) = tokio::join!(
        projector::run_single_batch(&fixture.cala, job_id, &config, &mut state_a),
        projector::run_single_batch(&fixture.cala, job_id, &config, &mut state_b),
    );

    let (winner_state, loser_result) = match (result_a, result_b) {
        (Ok(events), Err(loser)) => {
            assert!(events > 0);
            (state_a, loser)
        }
        (Err(loser), Ok(events)) => {
            assert!(events > 0);
            (state_b, loser)
        }
        (Ok(_), Ok(_)) => panic!("both racing runners committed"),
        (Err(a), Err(b)) => panic!("no racing runner committed: {a} / {b}"),
    };
    let _ = loser_result;

    let mut state = winner_state;
    while projector::run_single_batch(&fixture.cala, job_id, &config, &mut state).await? > 0 {}

    let ec_bal = fixture
        .cala
        .balances()
        .find(fixture.journal_id, fixture.ec_set, btc)
        .await?;
    assert_eq!(ec_bal.settled(), dec!(1290) * Decimal::from(3));
    assert_eq!(
        ec_bal.details.version, 3,
        "the version counts folded entries exactly once"
    );

    Ok(())
}

/// Crash-resume: fold a batch in an operation and drop it uncommitted;
/// the rerun refolds the same window exactly once.
#[tokio::test]
async fn crash_mid_batch_resumes_exactly_once() -> anyhow::Result<()> {
    use sqlx::Row as _;

    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let fixture = EcFixture::init(&pool, false).await?;

    let head = helpers::outbox_head(&pool).await?;
    let seed = ProjectorState { sequence: head };
    let job_id = seed_projector_job(&pool, &seed).await?;

    for _ in 0..2 {
        fixture.post().await?;
    }

    {
        let mut op = fixture.cala.begin_operation().await?;
        let mut crashed_state = seed;
        let events = projector::run_single_batch_in_op(
            &fixture.cala,
            &mut op,
            job_id,
            &direct_config(),
            &mut crashed_state,
        )
        .await?;
        assert!(events > 0, "the crashing batch must have consumed events");
        drop(op);
    }

    assert!(
        fixture
            .cala
            .balances()
            .find(fixture.journal_id, fixture.ec_set, btc)
            .await
            .is_err(),
        "a dropped operation must not leak snapshots"
    );
    let row = sqlx::query("SELECT execution_state_json FROM job_executions WHERE id = $1")
        .bind(uuid::Uuid::from(job_id))
        .fetch_one(&pool)
        .await?;
    let persisted: serde_json::Value = row.try_get("execution_state_json")?;
    assert_eq!(
        persisted["sequence"].as_i64(),
        Some(head),
        "a dropped operation must not advance the durable cursor"
    );

    let mut state = seed;
    let events =
        projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?;
    assert!(events > 0, "the resumed batch must refold the window");

    let ec_bal = fixture
        .cala
        .balances()
        .find(fixture.journal_id, fixture.ec_set, btc)
        .await?;
    let inline_bal = fixture
        .cala
        .balances()
        .find(fixture.journal_id, fixture.inline_set, btc)
        .await?;
    assert_eq!(ec_bal.settled(), inline_bal.settled());
    assert_eq!(ec_bal.details.version, 2);
    let ec_account = AccountId::from(&fixture.ec_set);
    assert_eq!(
        history_row_count(&pool, fixture.journal_id, ec_account, btc).await?,
        1,
        "one refolded batch coalesces to one BTC history row"
    );
    assert_eq!(
        history_row_count(&pool, fixture.journal_id, ec_account, usd).await?,
        1,
        "one refolded batch coalesces to one USD history row"
    );

    while projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?
        > 0
    {}
    let ec_after_feedback = fixture
        .cala
        .balances()
        .find(fixture.journal_id, fixture.ec_set, btc)
        .await?;
    assert_eq!(ec_after_feedback.details.version, 2);
    assert_eq!(
        history_row_count(&pool, fixture.journal_id, ec_account, btc).await?,
        1,
        "the projector's own Balance* feedback events advance the cursor only"
    );

    Ok(())
}

fn assert_balance_amounts_eq(
    actual: &cala_ledger::balance::AccountBalance,
    expected: &cala_ledger::balance::AccountBalance,
) {
    assert_eq!(actual.settled(), expected.settled());
    assert_eq!(actual.pending(), expected.pending());
    assert_eq!(actual.encumbrance(), expected.encumbrance());
}

fn all_balances_query<C: std::fmt::Debug>() -> es_entity::PaginatedQueryArgs<C> {
    es_entity::PaginatedQueryArgs {
        first: 100,
        after: None,
    }
}

fn balances_by_currency<C>(
    balances: es_entity::PaginatedQueryRet<cala_ledger::balance::AccountBalance, C>,
) -> std::collections::HashMap<Currency, cala_ledger::balance::AccountBalance> {
    balances
        .entities
        .into_iter()
        .map(|balance| (balance.details.currency, balance))
        .collect()
}

/// The main convergence proof: the EC set converges to the inline
/// reference in every currency, its version advances by the number of
/// entries folded, and one history row materializes per batch.
#[tokio::test]
async fn eventually_consistent_balances_converge() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let fixture = EcFixture::init(&pool, false).await?;

    let head = helpers::outbox_head(&pool).await?;
    let mut state = ProjectorState { sequence: head };
    let job_id = seed_projector_job(&pool, &state).await?;

    for _ in 0..2 {
        fixture.post().await?;
    }
    let events =
        projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?;
    assert!(events > 0);

    let journal_id = fixture.journal_id;
    let inline_account = AccountId::from(&fixture.inline_set);
    let ec_account = AccountId::from(&fixture.ec_set);

    let inline_btc = fixture
        .cala
        .balances()
        .find(journal_id, inline_account, btc)
        .await?;
    let ec_btc = fixture
        .cala
        .balances()
        .find(journal_id, ec_account, btc)
        .await?;
    assert_balance_amounts_eq(&ec_btc, &inline_btc);
    assert_eq!(ec_btc.details.version, inline_btc.details.version);
    assert_eq!(ec_btc.details.version, 2);

    let inline_usd = fixture
        .cala
        .balances()
        .find(journal_id, inline_account, usd)
        .await?;
    let ec_usd = fixture
        .cala
        .balances()
        .find(journal_id, ec_account, usd)
        .await?;
    assert_balance_amounts_eq(&ec_usd, &inline_usd);
    assert_eq!(ec_usd.details.version, inline_usd.details.version);

    assert_eq!(
        history_row_count(&pool, journal_id, ec_account, btc).await?,
        1
    );
    assert_eq!(
        history_row_count(&pool, journal_id, inline_account, btc).await?,
        2
    );
    assert_eq!(
        history_row_count(&pool, journal_id, ec_account, usd).await?,
        1
    );
    assert_eq!(
        history_row_count(&pool, journal_id, inline_account, usd).await?,
        4
    );

    for _ in 0..2 {
        fixture.post().await?;
    }
    let events =
        projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?;
    assert!(events > 0);

    let inline_btc_2 = fixture
        .cala
        .balances()
        .find(journal_id, inline_account, btc)
        .await?;
    let ec_btc_2 = fixture
        .cala
        .balances()
        .find(journal_id, ec_account, btc)
        .await?;
    assert_balance_amounts_eq(&ec_btc_2, &inline_btc_2);
    assert_eq!(ec_btc_2.details.version, inline_btc_2.details.version);
    assert!(
        ec_btc_2.details.version > ec_btc.details.version,
        "the version must be strictly monotonic across batches"
    );

    let inline_usd_2 = fixture
        .cala
        .balances()
        .find(journal_id, inline_account, usd)
        .await?;
    let ec_usd_2 = fixture
        .cala
        .balances()
        .find(journal_id, ec_account, usd)
        .await?;
    assert_balance_amounts_eq(&ec_usd_2, &inline_usd_2);

    assert_eq!(
        history_row_count(&pool, journal_id, ec_account, btc).await?,
        2,
        "two bursts folded in two batches coalesce to two rows"
    );
    assert_eq!(
        history_row_count(&pool, journal_id, inline_account, btc).await?,
        4,
        "the inline reference writes one row per entry"
    );

    Ok(())
}

/// Shared-member hierarchy through the live job: two EC child sets with
/// exclusive leaves under an EC root converge so that the root equals the
/// sum of its children.
#[tokio::test]
async fn shared_member_hierarchy_converges() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, recipient) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let recipient = cala.accounts().create(recipient).await?;
    let extra = cala
        .accounts()
        .create(
            NewAccount::builder()
                .id(uuid::Uuid::now_v7())
                .name("Extra Account")
                .code(Alphanumeric.sample_string(&mut rand::rng(), 32))
                .build()
                .unwrap(),
        )
        .await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let set_a = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Set A"))
        .await?;
    let set_b = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Set B"))
        .await?;
    let root = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Root"))
        .await?;
    cala.account_sets()
        .add_member(set_a.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(set_b.id(), extra.id())
        .await?;
    cala.account_sets()
        .add_member(root.id(), set_a.id())
        .await?;
    cala.account_sets()
        .add_member(root.id(), set_b.id())
        .await?;

    for _ in 0..3 {
        post_conversion(&cala, journal.id(), &tx_code, sender.id(), recipient.id()).await?;
    }
    for _ in 0..2 {
        post_conversion(&cala, journal.id(), &tx_code, extra.id(), sender.id()).await?;
    }

    let recipient_bal = cala
        .balances()
        .find(journal.id(), recipient.id(), btc)
        .await?;
    let extra_bal = cala.balances().find(journal.id(), extra.id(), btc).await?;

    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        set_a.id(),
        btc,
        recipient_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        set_b.id(),
        btc,
        extra_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        root.id(),
        btc,
        recipient_bal.settled() + extra_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;

    cala.shutdown().await?;
    Ok(())
}

/// Depth > 1 through the live job: the transitive-closure membership table
/// carries the leaf to both the child and the root, so both converge to
/// the leaf's balance.
#[tokio::test]
async fn deep_hierarchy_converges() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, recipient) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let recipient = cala.accounts().create(recipient).await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let child = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Child"))
        .await?;
    let root = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Root"))
        .await?;
    cala.account_sets()
        .add_member(child.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(root.id(), child.id())
        .await?;

    for _ in 0..3 {
        post_conversion(&cala, journal.id(), &tx_code, sender.id(), recipient.id()).await?;
    }

    let recipient_bal = cala
        .balances()
        .find(journal.id(), recipient.id(), btc)
        .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        child.id(),
        btc,
        recipient_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        root.id(),
        btc,
        recipient_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;

    cala.shutdown().await?;
    Ok(())
}

/// A hierarchy mixing EC and inline sets: the projector folds only the EC
/// nodes while the poster path keeps maintaining the inline child, and the
/// EC root still sees both transitive leaves.
#[tokio::test]
async fn mixed_hierarchy_converges() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, leaf_a) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let leaf_a = cala.accounts().create(leaf_a).await?;
    let leaf_b = cala
        .accounts()
        .create(
            NewAccount::builder()
                .id(uuid::Uuid::now_v7())
                .name("Leaf B")
                .code(Alphanumeric.sample_string(&mut rand::rng(), 32))
                .build()
                .unwrap(),
        )
        .await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let ec_root = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "EC Root"))
        .await?;
    let ec_child = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "EC Child"))
        .await?;
    let inline_child = cala
        .account_sets()
        .create(new_inline_set(journal.id(), "Inline Child"))
        .await?;
    cala.account_sets()
        .add_member(ec_child.id(), leaf_a.id())
        .await?;
    cala.account_sets()
        .add_member(inline_child.id(), leaf_b.id())
        .await?;
    cala.account_sets()
        .add_member(ec_root.id(), ec_child.id())
        .await?;
    cala.account_sets()
        .add_member(ec_root.id(), inline_child.id())
        .await?;

    post_conversion(&cala, journal.id(), &tx_code, sender.id(), leaf_a.id()).await?;
    post_conversion(&cala, journal.id(), &tx_code, sender.id(), leaf_b.id()).await?;

    let leaf_a_bal = cala.balances().find(journal.id(), leaf_a.id(), btc).await?;
    let leaf_b_bal = cala.balances().find(journal.id(), leaf_b.id(), btc).await?;
    let inline_after_posts = cala
        .balances()
        .find(journal.id(), inline_child.id(), btc)
        .await?;
    assert_eq!(
        inline_after_posts.settled(),
        leaf_b_bal.settled(),
        "the poster maintains the inline child synchronously"
    );

    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        ec_child.id(),
        btc,
        leaf_a_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        ec_root.id(),
        btc,
        leaf_a_bal.settled() + leaf_b_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;

    let inline_after_convergence = cala
        .balances()
        .find(journal.id(), inline_child.id(), btc)
        .await?;
    assert_eq!(
        inline_after_posts.details.version, inline_after_convergence.details.version,
        "the projector must never touch a non-EC set"
    );

    cala.shutdown().await?;
    Ok(())
}

/// Listing balances for an EC set once the projector has converged,
/// through the same listing APIs used for any other account.
#[tokio::test]
async fn list_current_balances_for_eventually_consistent_account_set() -> anyhow::Result<()> {
    use std::collections::HashSet;

    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, receiver) = helpers::test_accounts();
    let sender_account = cala.accounts().create(sender).await?;
    let recipient_one = cala.accounts().create(receiver).await?;
    let (_, receiver_two) = helpers::test_accounts();
    let recipient_two = cala.accounts().create(receiver_two).await?;

    let inline_set = cala
        .account_sets()
        .create(new_inline_set(journal.id(), "Inline Set"))
        .await?;
    let ec_set = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "EC Set"))
        .await?;
    cala.account_sets()
        .add_member(inline_set.id(), recipient_one.id())
        .await?;
    cala.account_sets()
        .add_member(inline_set.id(), recipient_two.id())
        .await?;
    cala.account_sets()
        .add_member(ec_set.id(), recipient_one.id())
        .await?;
    cala.account_sets()
        .add_member(ec_set.id(), recipient_two.id())
        .await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    post_conversion(
        &cala,
        journal.id(),
        &tx_code,
        sender_account.id(),
        recipient_one.id(),
    )
    .await?;
    post_conversion(
        &cala,
        journal.id(),
        &tx_code,
        sender_account.id(),
        recipient_two.id(),
    )
    .await?;

    let inline_balances = cala
        .balances()
        .list_for_account(
            journal.id(),
            AccountId::from(inline_set.id()),
            all_balances_query(),
        )
        .await?;
    let inline_balances = balances_by_currency(inline_balances);

    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        ec_set.id(),
        btc,
        inline_balances[&btc].settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        ec_set.id(),
        usd,
        inline_balances[&usd].settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;

    let ec_balances = cala
        .balances()
        .list_for_account(
            journal.id(),
            AccountId::from(ec_set.id()),
            all_balances_query(),
        )
        .await?;
    let ec_balances = balances_by_currency(ec_balances);
    let currencies: HashSet<_> = ec_balances.keys().copied().collect();
    assert_eq!(currencies, HashSet::from([btc, usd]));

    let ec_btc = cala.balances().find(journal.id(), ec_set.id(), btc).await?;
    assert_eq!(ec_balances[&btc].balance_type, ec_btc.balance_type);
    assert_eq!(ec_balances[&btc].details, ec_btc.details);

    let ec_usd = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(ec_balances[&usd].balance_type, ec_usd.balance_type);
    assert_eq!(ec_balances[&usd].details, ec_usd.details);

    for currency in [btc, usd] {
        assert_balance_amounts_eq(&ec_balances[&currency], &inline_balances[&currency]);
    }

    cala.shutdown().await?;
    Ok(())
}

/// The effective dimension through the live job: cumulative-by-date EC
/// balances converge to the inline reference, both as point lookups and
/// through the cumulative listing and range APIs.
#[tokio::test]
async fn ec_effective_balances_converge() -> anyhow::Result<()> {
    use std::collections::HashSet;

    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala
        .journals()
        .create(helpers::test_journal_with_effective_balances())
        .await?;
    let (sender, receiver) = helpers::test_accounts();
    let sender_account = cala.accounts().create(sender).await?;
    let recipient = cala.accounts().create(receiver).await?;

    let inline_set = cala
        .account_sets()
        .create(new_inline_set(journal.id(), "Inline Set"))
        .await?;
    let ec_set = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "EC Set"))
        .await?;
    cala.account_sets()
        .add_member(inline_set.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(ec_set.id(), recipient.id())
        .await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let date = chrono::NaiveDate::from_ymd_opt(2025, 6, 12).unwrap();
    for _ in 0..2 {
        let mut params = Params::new();
        params.insert("journal_id", journal.id());
        params.insert("sender", sender_account.id());
        params.insert("recipient", recipient.id());
        params.insert("effective", date);
        cala.post_transaction(TransactionId::new(), &tx_code, params)
            .await?;
    }

    let inline_btc = cala
        .balances()
        .effective()
        .find_cumulative(journal.id(), inline_set.id(), btc, date)
        .await?;
    let inline_usd = cala
        .balances()
        .effective()
        .find_cumulative(journal.id(), inline_set.id(), usd, date)
        .await?;

    let ec_btc = helpers::wait_for_ec_effective_convergence(
        &cala,
        journal.id(),
        ec_set.id(),
        btc,
        date,
        inline_btc.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    let ec_usd = helpers::wait_for_ec_effective_convergence(
        &cala,
        journal.id(),
        ec_set.id(),
        usd,
        date,
        inline_usd.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    assert_balance_amounts_eq(&ec_btc, &inline_btc);
    assert_balance_amounts_eq(&ec_usd, &inline_usd);

    let ec_cumulative = cala
        .balances()
        .effective()
        .list_cumulative_for_account(
            journal.id(),
            AccountId::from(ec_set.id()),
            date,
            all_balances_query(),
        )
        .await?;
    let ec_cumulative = balances_by_currency(ec_cumulative);
    let currencies: HashSet<_> = ec_cumulative.keys().copied().collect();
    assert_eq!(currencies, HashSet::from([btc, usd]));
    assert_balance_amounts_eq(&ec_cumulative[&btc], &inline_btc);
    assert_balance_amounts_eq(&ec_cumulative[&usd], &inline_usd);

    let inline_ranges = cala
        .balances()
        .effective()
        .list_in_range_for_account(
            journal.id(),
            AccountId::from(inline_set.id()),
            date,
            Some(date),
            all_balances_query(),
        )
        .await?;
    let ec_ranges = cala
        .balances()
        .effective()
        .list_in_range_for_account(
            journal.id(),
            AccountId::from(ec_set.id()),
            date,
            Some(date),
            all_balances_query(),
        )
        .await?;
    assert_eq!(ec_ranges.entities.len(), inline_ranges.entities.len());
    for (ec_range, inline_range) in ec_ranges.entities.iter().zip(inline_ranges.entities.iter()) {
        assert_balance_amounts_eq(&ec_range.close, &inline_range.close);
        assert_balance_amounts_eq(&ec_range.period, &inline_range.period);
    }

    cala.shutdown().await?;
    Ok(())
}

/// Backdated-effective convergence, driven batch by batch: in-order dates
/// converge first, then a backdated posting lands and the scoped rebuild
/// rewrites the backdated date and every later date.
#[tokio::test]
async fn backdated_effective_converges() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let fixture = EcFixture::init(&pool, true).await?;

    let head = helpers::outbox_head(&pool).await?;
    let mut state = ProjectorState { sequence: head };
    let job_id = seed_projector_job(&pool, &state).await?;

    let date1 = chrono::NaiveDate::from_ymd_opt(2025, 3, 10).unwrap();
    let date2 = chrono::NaiveDate::from_ymd_opt(2025, 3, 20).unwrap();
    let date3 = chrono::NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();

    fixture.post_effective(date1).await?;
    fixture.post_effective(date2).await?;
    let events =
        projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?;
    assert!(events > 0);

    let journal_id = fixture.journal_id;
    let inline = AccountId::from(&fixture.inline_set);
    let ec = AccountId::from(&fixture.ec_set);
    let effective = fixture.cala.balances().effective();

    for date in [date1, date2] {
        let inline_bal = effective
            .find_cumulative(journal_id, inline, btc, date)
            .await?;
        let ec_bal = effective.find_cumulative(journal_id, ec, btc, date).await?;
        assert_balance_amounts_eq(&ec_bal, &inline_bal);
    }
    let inline_usd = effective
        .find_cumulative(journal_id, inline, usd, date2)
        .await?;
    let ec_usd = effective
        .find_cumulative(journal_id, ec, usd, date2)
        .await?;
    assert_balance_amounts_eq(&ec_usd, &inline_usd);

    fixture.post_effective(date3).await?;
    let events =
        projector::run_single_batch(&fixture.cala, job_id, &direct_config(), &mut state).await?;
    assert!(events > 0);

    for date in [date1, date3, date2] {
        let inline_bal = effective
            .find_cumulative(journal_id, inline, btc, date)
            .await?;
        let ec_bal = effective.find_cumulative(journal_id, ec, btc, date).await?;
        assert_balance_amounts_eq(&ec_bal, &inline_bal);
    }
    let inline_d3 = effective
        .find_cumulative(journal_id, inline, btc, date3)
        .await?;
    assert_eq!(inline_d3.settled(), dec!(2580), "date1 + backdated date3");

    Ok(())
}

/// The reversed ordering of the guard regression: wiring the subtree while
/// it is history-free is permitted, and the projector then converges the
/// parent through the pre-wired closure.
#[tokio::test]
async fn attached_subtree_converges() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();
    let pool = helpers::init_pool().await?;
    let cala = helpers::init_cala_with_projector(pool.clone(), true).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, leaf) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let leaf = cala.accounts().create(leaf).await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let parent = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Attach First Parent"))
        .await?;
    let child = cala
        .account_sets()
        .create(new_ec_set(journal.id(), "Attach First Child"))
        .await?;
    cala.account_sets()
        .add_member(child.id(), leaf.id())
        .await?;
    cala.account_sets()
        .add_member(parent.id(), child.id())
        .await?;

    for _ in 0..2 {
        post_conversion(&cala, journal.id(), &tx_code, sender.id(), leaf.id()).await?;
    }

    let leaf_bal = cala.balances().find(journal.id(), leaf.id(), btc).await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        child.id(),
        btc,
        leaf_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;
    helpers::wait_for_ec_convergence(
        &cala,
        journal.id(),
        parent.id(),
        btc,
        leaf_bal.settled(),
        CONVERGENCE_TIMEOUT,
    )
    .await?;

    cala.shutdown().await?;
    Ok(())
}

/// Pins the intra-transaction outbox ordering the projector reads:
/// TransactionCreated, then that transaction's EntryCreated events in
/// entry order, then the poster's Balance* events.
#[tokio::test]
async fn outbox_order_pins_transaction_then_entries_then_balances() -> anyhow::Result<()> {
    use futures::StreamExt;

    let pool = helpers::init_pool().await?;
    let cala = direct_drive_ledger(&pool).await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, receiver) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let recipient = cala.accounts().create(receiver).await?;

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    let mut listener = cala.register_outbox_listener(None);

    let tx_id = TransactionId::new();
    let mut params = Params::new();
    params.insert("journal_id", journal.id().to_string());
    params.insert("sender", sender.id());
    params.insert("recipient", recipient.id());
    cala.post_transaction(tx_id, &tx_code, params).await?;

    let mut my_entry_ids = std::collections::HashSet::new();
    let mut tx_created_seq: Option<u64> = None;
    let mut entry_events: Vec<(u64, u32)> = Vec::new();
    let mut balance_seqs: Vec<u64> = Vec::new();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let Ok(next) = tokio::time::timeout(Duration::from_millis(500), listener.next()).await
        else {
            if tx_created_seq.is_some() && entry_events.len() == 6 && !balance_seqs.is_empty() {
                break;
            }
            continue;
        };
        let Some(event) = next else { break };
        let sequence = u64::from(event.sequence);
        match event.payload.as_ref() {
            Some(outbox::OutboxEventPayload::TransactionCreated { transaction })
                if transaction.id == tx_id =>
            {
                tx_created_seq = Some(sequence);
                my_entry_ids.extend(transaction.entry_ids.iter().copied());
            }
            Some(outbox::OutboxEventPayload::EntryCreated { entry })
                if entry.transaction_id == tx_id =>
            {
                my_entry_ids.insert(entry.id);
                entry_events.push((sequence, entry.sequence));
            }
            Some(outbox::OutboxEventPayload::BalanceCreated { balance })
                if my_entry_ids.contains(&balance.entry_id) =>
            {
                balance_seqs.push(sequence);
            }
            Some(outbox::OutboxEventPayload::BalanceUpdated { balance })
                if my_entry_ids.contains(&balance.entry_id) =>
            {
                balance_seqs.push(sequence);
            }
            _ => {}
        }
    }

    let tx_created_seq = tx_created_seq.expect("must observe TransactionCreated");
    assert_eq!(entry_events.len(), 6, "must observe all six EntryCreated");
    assert!(!balance_seqs.is_empty(), "must observe Balance* events");

    let first_entry_seq = entry_events.iter().map(|(seq, _)| *seq).min().unwrap();
    let last_entry_seq = entry_events.iter().map(|(seq, _)| *seq).max().unwrap();
    assert!(
        tx_created_seq < first_entry_seq,
        "TransactionCreated must precede its entries"
    );
    let mut sorted_by_outbox = entry_events.clone();
    sorted_by_outbox.sort_by_key(|(seq, _)| *seq);
    assert!(
        sorted_by_outbox
            .windows(2)
            .all(|pair| pair[0].1 < pair[1].1),
        "EntryCreated events must arrive in entry-sequence order"
    );
    assert!(
        balance_seqs.iter().all(|seq| *seq > last_entry_seq),
        "Balance* events must follow all entries"
    );

    Ok(())
}
