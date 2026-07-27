#![allow(dead_code)]
use rand::distr::{Alphanumeric, SampleString};

use std::time::Duration;

use cala_ledger::{
    account::*,
    account_set::NewAccountSet,
    balance::{error::BalanceError, AccountBalance},
    journal::*,
    primitives::{AccountId, BalanceRollup, Currency, JournalId},
    projector::{BalanceProjectorConfig, BalanceProjectorInit},
    tx_template::*,
    CalaLedger, CalaLedgerConfig,
};
use chrono::NaiveDate;
use rust_decimal::Decimal;

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    init_pool_with(sqlx::postgres::PgPoolOptions::new()).await
}

/// Same as `init_pool`, but lets the caller pre-configure the pool (max
/// connections, acquire timeout, etc.) for tests that need more headroom.
pub async fn init_pool_with(
    options: sqlx::postgres::PgPoolOptions,
) -> anyhow::Result<sqlx::PgPool> {
    let pg_con = std::env::var("PG_CON").unwrap();
    let pool = options.connect(&pg_con).await?;
    use job::IncludeMigrations;
    sqlx::migrate!().include_job_migrations().run(&pool).await?;
    Ok(pool)
}

pub async fn outbox_head(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    use sqlx::Row as _;
    let row = sqlx::query(
        "SELECT COALESCE(MAX(sequence), 0)::bigint AS head FROM cala_persistent_outbox_events",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("head")?)
}

/// Initialize a `CalaLedger` with the balance projector flag set. The
/// poller is tuned for cross-test handoff on a shared dev database, and
/// the durable job's cursor is seeded at the current outbox head (test
/// isolation, not production behaviour: each run tails only its own
/// activity instead of replaying every other test's). Established
/// cursors are left untouched.
pub async fn init_cala_with_projector(
    pool: sqlx::PgPool,
    enabled: bool,
) -> anyhow::Result<CalaLedger> {
    if enabled {
        seed_projector_cursor_at_head(&pool).await?;
    }
    let poller_config = job::JobPollerConfig {
        job_lost_interval: Duration::from_secs(30),
        pending_jobs_check_interval: Duration::from_secs(1),
        ..Default::default()
    };
    let config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .ec_balance_projector(enabled)
        .job_poller_config(poller_config)
        .build()?;
    Ok(CalaLedger::init(config).await?)
}

async fn seed_projector_cursor_at_head(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let head = outbox_head(pool).await?;
    let seed_config = CalaLedgerConfig::builder()
        .pool(pool.clone())
        .exec_migrations(false)
        .build()?;
    let seed_ledger = CalaLedger::init(seed_config).await?;
    let mut jobs = job::Jobs::init(
        job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .map_err(anyhow::Error::msg)?,
    )
    .await?;
    let spawner = jobs.add_initializer(BalanceProjectorInit::new(&seed_ledger));
    spawner
        .spawn_unique(job::JobId::new(), BalanceProjectorConfig::default())
        .await?;
    sqlx::query(
        "UPDATE job_executions SET execution_state_json = jsonb_build_object('sequence', $1::bigint) \
         WHERE job_type = 'balance-projector' AND execution_state_json IS NULL",
    )
    .bind(head)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn wait_for_ec_convergence(
    cala: &CalaLedger,
    journal_id: JournalId,
    account_id: impl Into<AccountId> + Copy,
    currency: Currency,
    expected_settled: Decimal,
    timeout: Duration,
) -> anyhow::Result<AccountBalance> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_seen: Option<Decimal> = None;
    loop {
        match cala
            .balances()
            .find(journal_id, account_id.into(), currency)
            .await
        {
            Ok(balance) => {
                if balance.settled() == expected_settled {
                    return Ok(balance);
                }
                last_seen = Some(balance.settled());
            }
            Err(BalanceError::NotFound(..)) => {}
            Err(e) => return Err(e.into()),
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "EC balance did not converge to {expected_settled} within {timeout:?} \
                 (last seen: {last_seen:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn wait_for_ec_effective_convergence(
    cala: &CalaLedger,
    journal_id: JournalId,
    account_id: impl Into<AccountId> + Copy,
    currency: Currency,
    date: NaiveDate,
    expected_settled: Decimal,
    timeout: Duration,
) -> anyhow::Result<AccountBalance> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_seen: Option<Decimal> = None;
    loop {
        match cala
            .balances()
            .effective()
            .find_cumulative(journal_id, account_id.into(), currency, date)
            .await
        {
            Ok(balance) => {
                if balance.settled() == expected_settled {
                    return Ok(balance);
                }
                last_seen = Some(balance.settled());
            }
            Err(BalanceError::NotFound(..)) => {}
            Err(e) => return Err(e.into()),
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!(
                "EC effective balance at {date} did not converge to {expected_settled} \
                 within {timeout:?} (last seen: {last_seen:?})"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub fn test_journal() -> NewJournal {
    let name = Alphanumeric.sample_string(&mut rand::rng(), 32);
    NewJournal::builder()
        .id(JournalId::new())
        .name(name)
        .build()
        .unwrap()
}

pub fn test_journal_with_effective_balances() -> NewJournal {
    let name = Alphanumeric.sample_string(&mut rand::rng(), 32);
    NewJournal::builder()
        .id(JournalId::new())
        .name(name)
        .enable_effective_balance(true)
        .build()
        .unwrap()
}

pub fn test_accounts() -> (NewAccount, NewAccount) {
    let code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let sender_account = NewAccount::builder()
        .id(uuid::Uuid::now_v7())
        .name(format!("Test Sender Account {code}"))
        .code(code)
        .build()
        .unwrap();

    let code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let recipient_account = NewAccount::builder()
        .id(uuid::Uuid::now_v7())
        .name(format!("Test Recipient Account {code}"))
        .code(code)
        .build()
        .unwrap();
    (sender_account, recipient_account)
}

pub fn test_account_sets(journal_id: uuid::Uuid) -> (NewAccountSet, NewAccountSet) {
    let code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let sender_account_set = NewAccountSet::builder()
        .id(uuid::Uuid::now_v7())
        .name(format!("Test Sender Account Set {code}"))
        .journal_id(journal_id)
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();

    let code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let recipient_account_set = NewAccountSet::builder()
        .id(uuid::Uuid::now_v7())
        .name(format!("Test Recipient Account Set {code}"))
        .journal_id(journal_id)
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();

    (sender_account_set, recipient_account_set)
}

pub fn currency_conversion_template(code: &str) -> NewTxTemplate {
    let params = vec![
        NewParamDefinition::builder()
            .name("recipient")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("sender")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("journal_id")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("effective")
            .r#type(ParamDataType::Date)
            .default_expr("date()")
            .build()
            .unwrap(),
    ];
    let entries = vec![
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_BTC_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('1290')")
            .currency("'BTC'")
            .metadata(r#"{"sender": params.sender}"#)
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_BTC_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("decimal('1290')")
            .currency("'BTC'")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_USD_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_USD_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_USD_PENDING_DR'")
            .account_id("params.sender")
            .layer("PENDING")
            .direction("DEBIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_USD_PENDING_CR'")
            .account_id("params.recipient")
            .layer("PENDING")
            .direction("CREDIT")
            .units("decimal('100')")
            .currency("'USD'")
            .build()
            .unwrap(),
    ];
    NewTxTemplate::builder()
        .id(uuid::Uuid::now_v7())
        .code(code)
        .params(params)
        .transaction(
            NewTxTemplateTransaction::builder()
                .effective("params.effective")
                .journal_id("params.journal_id")
                .metadata(r#"{"foo": "bar"}"#)
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap()
}

pub fn simple_template_with_date_default(code: &str) -> NewTxTemplate {
    let params = vec![
        NewParamDefinition::builder()
            .name("recipient")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("sender")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("journal_id")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("amount")
            .r#type(ParamDataType::Decimal)
            .build()
            .unwrap(),
    ];
    let entries = vec![
        NewTxTemplateEntry::builder()
            .entry_type("'CLOCK_TEST_DR'")
            .account_id("params.sender")
            .layer("SETTLED")
            .direction("DEBIT")
            .units("params.amount")
            .currency("'USD'")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'CLOCK_TEST_CR'")
            .account_id("params.recipient")
            .layer("SETTLED")
            .direction("CREDIT")
            .units("params.amount")
            .currency("'USD'")
            .build()
            .unwrap(),
    ];
    NewTxTemplate::builder()
        .id(uuid::Uuid::now_v7())
        .code(code)
        .params(params)
        .transaction(
            NewTxTemplateTransaction::builder()
                .effective("date()")
                .journal_id("params.journal_id")
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap()
}

pub fn velocity_template(code: &str) -> NewTxTemplate {
    let params = vec![
        NewParamDefinition::builder()
            .name("recipient")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("sender")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("journal_id")
            .r#type(ParamDataType::Uuid)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("amount")
            .r#type(ParamDataType::Decimal)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("currency")
            .r#type(ParamDataType::String)
            .default_expr("'USD'")
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("layer")
            .r#type(ParamDataType::String)
            .default_expr("'SETTLED'")
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("meta")
            .r#type(ParamDataType::Json)
            .default_expr(r#"{"foo": "bar"}"#)
            .build()
            .unwrap(),
        NewParamDefinition::builder()
            .name("effective")
            .r#type(ParamDataType::Date)
            .default_expr("date()")
            .build()
            .unwrap(),
    ];
    let entries = vec![
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_DR'")
            .account_id("params.sender")
            .layer("params.layer")
            .direction("DEBIT")
            .units("params.amount")
            .currency("params.currency")
            .build()
            .unwrap(),
        NewTxTemplateEntry::builder()
            .entry_type("'TEST_CR'")
            .account_id("params.recipient")
            .layer("params.layer")
            .direction("CREDIT")
            .units("params.amount")
            .currency("params.currency")
            .build()
            .unwrap(),
    ];
    NewTxTemplate::builder()
        .id(uuid::Uuid::now_v7())
        .code(code)
        .params(params)
        .transaction(
            NewTxTemplateTransaction::builder()
                .effective("params.effective")
                .journal_id("params.journal_id")
                .metadata("params.meta")
                .build()
                .unwrap(),
        )
        .entries(entries)
        .build()
        .unwrap()
}
