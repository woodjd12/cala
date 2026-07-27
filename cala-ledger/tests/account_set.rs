mod helpers;

use rand::distr::{Alphanumeric, SampleString};

use cala_ledger::{
    account::*, account_set::error::AccountSetError, account_set::*, tx_template::*, *,
};

#[tokio::test]
async fn errors_on_collision() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let (one, two) = helpers::test_accounts();
    let one = cala.accounts().create(one).await.unwrap();
    let two = cala.accounts().create(two).await.unwrap();

    let new_journal = helpers::test_journal();
    let journal = cala.journals().create(new_journal).await.unwrap();

    let set_one = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("SET ONE")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let set_one = cala.account_sets().create(set_one).await.unwrap();

    let set_two = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("SET TWO")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let set_two = cala.account_sets().create(set_two).await.unwrap();

    let parent = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("parent")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let parent = cala.account_sets().create(parent).await.unwrap();

    // Cannot add the same account twice
    let res = cala.account_sets().add_member(set_one.id(), one.id()).await;
    assert!(res.is_ok());
    let res = cala.account_sets().add_member(set_one.id(), one.id()).await;
    assert!(res.is_err());

    // Cannot add an account included in child
    let res = cala
        .account_sets()
        .add_member(parent.id(), set_one.id())
        .await;
    assert!(res.is_ok());
    let res = cala.account_sets().add_member(parent.id(), one.id()).await;
    assert!(res.is_err());

    let res = cala.account_sets().add_member(set_two.id(), two.id()).await;
    assert!(res.is_ok());
    let res = cala.account_sets().add_member(parent.id(), two.id()).await;
    assert!(res.is_ok());

    // Cannot add an account included in sibling
    let res = cala.account_sets().add_member(set_one.id(), two.id()).await;
    assert!(res.is_err());

    // remove one from set_one
    let res = cala
        .account_sets()
        .remove_member(set_one.id(), one.id())
        .await;
    assert!(res.is_ok());

    // can add one to parent set
    let res = cala.account_sets().add_member(parent.id(), one.id()).await;
    assert!(res.is_ok());

    Ok(())
}

#[tokio::test]
async fn balances() -> anyhow::Result<()> {
    let btc: Currency = "BTC".parse().unwrap();

    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let new_journal = helpers::test_journal();
    let journal = cala.journals().create(new_journal).await.unwrap();

    let (sender, receiver) = helpers::test_accounts();
    let sender_account = cala.accounts().create(sender).await.unwrap();
    let recipient_account = cala.accounts().create(receiver).await.unwrap();

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let new_template = helpers::currency_conversion_template(&tx_code);
    cala.tx_templates().create(new_template).await.unwrap();

    let recipient_set = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Recipient Set")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let sender_set = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Sender Set")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let parent_set = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Parent")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    // Wire the hierarchy up *before* any posts so the no-history rule
    // is satisfied for every membership change.
    cala.account_sets()
        .add_member(recipient_set.id(), recipient_account.id())
        .await
        .unwrap();
    cala.account_sets()
        .add_member(sender_set.id(), sender_account.id())
        .await
        .unwrap();
    cala.account_sets()
        .add_member(parent_set.id(), recipient_set.id())
        .await
        .unwrap();
    cala.account_sets()
        .add_member(parent_set.id(), sender_set.id())
        .await
        .unwrap();

    let mut params = Params::new();
    params.insert("journal_id", journal.id().to_string());
    params.insert("sender", sender_account.id());
    params.insert("recipient", recipient_account.id());
    cala.post_transaction(TransactionId::new(), &tx_code, params)
        .await
        .unwrap();

    // Each direct parent fold-up matches its single member exactly.
    let recipient_balance = cala
        .balances()
        .find(journal.id(), recipient_account.id(), btc)
        .await?;
    let recipient_set_balance = cala
        .balances()
        .find(journal.id(), recipient_set.id(), btc)
        .await?;
    assert_eq!(recipient_balance.settled(), recipient_set_balance.settled());
    assert_eq!(
        recipient_balance.details.entry_id,
        recipient_set_balance.details.entry_id
    );

    let sender_balance = cala
        .balances()
        .find(journal.id(), sender_account.id(), btc)
        .await?;
    let sender_set_balance = cala
        .balances()
        .find(journal.id(), sender_set.id(), btc)
        .await?;
    assert_eq!(sender_balance.settled(), sender_set_balance.settled());
    assert_eq!(
        sender_balance.details.entry_id,
        sender_set_balance.details.entry_id
    );

    // The grandparent receives both sides of the same transaction, so
    // its settled balance is zero.
    let parent_balance = cala
        .balances()
        .find(journal.id(), parent_set.id(), btc)
        .await?;
    assert_eq!(parent_balance.settled(), rust_decimal::Decimal::ZERO);

    let query_args = es_entity::PaginatedQueryArgs {
        first: 2,
        after: None,
    };
    let ret = cala
        .entries()
        .list_for_account_set_id(
            recipient_set.id(),
            query_args,
            es_entity::ListDirection::Ascending,
        )
        .await?;

    assert!(!ret.entities.is_empty());
    Ok(())
}

#[tokio::test]
async fn account_set_update() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let new_journal = helpers::test_journal();
    let journal = cala.journals().create(new_journal).await.unwrap();

    // create account set
    let initial_name = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let new_account_set = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name(initial_name.clone())
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()?;

    let mut account_set = cala.account_sets().create(new_account_set).await?;
    assert_eq!(initial_name, account_set.values().name);

    // update account set name and description
    let updated_name = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let mut builder = AccountSetUpdate::default();
    builder.name(updated_name.clone()).build()?;
    if account_set.update(builder).did_execute() {
        cala.account_sets().persist(&mut account_set).await?;
    }
    assert_eq!(updated_name, account_set.values().name);
    Ok(())
}

#[tokio::test]
async fn members_pagination() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;
    let new_journal = helpers::test_journal();
    let journal = cala.journals().create(new_journal).await.unwrap();

    let (one, two) = helpers::test_accounts();
    let account_one = cala.accounts().create(one).await.unwrap();
    let account_two = cala.accounts().create(two).await.unwrap();

    let set_one = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("SET ONE")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let set_one = cala.account_sets().create(set_one).await.unwrap();
    let set_two = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("SET TWO")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let set_two = cala.account_sets().create(set_two).await.unwrap();

    let parent = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("parent")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let parent = cala.account_sets().create(parent).await.unwrap();

    cala.account_sets()
        .add_member(parent.id(), account_two.id())
        .await
        .unwrap();

    cala.account_sets()
        .add_member(parent.id(), set_one.id())
        .await
        .unwrap();

    cala.account_sets()
        .add_member(parent.id(), account_one.id())
        .await
        .unwrap();

    cala.account_sets()
        .add_member(parent.id(), set_two.id())
        .await
        .unwrap();

    let query_args = es_entity::PaginatedQueryArgs {
        first: 2,
        after: None,
    };

    let ret = cala
        .account_sets()
        .list_members_by_created_at(parent.id(), query_args)
        .await?;

    assert_eq!(ret.entities.len(), 2);
    assert!(ret.has_next_page);
    assert_eq!(
        ret.entities[0].id.clone(),
        AccountSetMemberId::from(set_two.id())
    );
    assert_eq!(
        ret.entities[1].id.clone(),
        AccountSetMemberId::from(account_one.id())
    );

    let query_args = es_entity::PaginatedQueryArgs {
        first: 2,
        after: Some(AccountSetMemberByCreatedAtCursor::from(&ret.entities[0])),
    };

    let ret = cala
        .account_sets()
        .list_members_by_created_at(parent.id(), query_args)
        .await?;
    assert_eq!(ret.entities.len(), 2);
    assert!(ret.has_next_page);
    assert_eq!(
        ret.entities[0].id.clone(),
        AccountSetMemberId::from(account_one.id())
    );
    assert_eq!(
        ret.entities[1].id.clone(),
        AccountSetMemberId::from(set_one.id())
    );

    let query_args = es_entity::PaginatedQueryArgs {
        first: 2,
        after: Some(AccountSetMemberByCreatedAtCursor::from(&ret.entities[1])),
    };

    let ret = cala
        .account_sets()
        .list_members_by_created_at(parent.id(), query_args)
        .await?;
    assert_eq!(ret.entities.len(), 1);
    assert!(!ret.has_next_page);
    assert_eq!(
        ret.entities[0].id.clone(),
        AccountSetMemberId::from(account_two.id())
    );

    Ok(())
}

#[tokio::test]
async fn list_members_by_external_id() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool)
            .exec_migrations(false)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let parent = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Parent Set")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()?,
        )
        .await?;

    let random = Alphanumeric.sample_string(&mut rand::rng(), 8);

    let account_ids = [
        cala.accounts()
            .create(
                NewAccount::builder()
                    .id(AccountId::new())
                    .name(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .code(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .external_id(format!("a-{random}"))
                    .build()?,
            )
            .await?,
        cala.accounts()
            .create(
                NewAccount::builder()
                    .id(AccountId::new())
                    .name(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .code(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .external_id(format!("z-{random}"))
                    .build()?,
            )
            .await?,
        cala.accounts()
            .create(
                NewAccount::builder()
                    .id(AccountId::new())
                    .name(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .code(Alphanumeric.sample_string(&mut rand::rng(), 8))
                    .build()?,
            )
            .await?,
    ];

    for account in &account_ids {
        cala.account_sets()
            .add_member(parent.id(), account.id())
            .await?;
    }

    let query_args = es_entity::PaginatedQueryArgs {
        first: 1,
        after: None,
    };
    let ret = cala
        .account_sets()
        .list_members_by_external_id(parent.id(), query_args)
        .await?;
    assert_eq!(ret.entities[0].external_id, Some(format!("a-{random}")));

    let query_args = es_entity::PaginatedQueryArgs {
        first: 1,
        after: Some(AccountSetMemberByExternalIdCursor::from(&ret.entities[0])),
    };
    let ret = cala
        .account_sets()
        .list_members_by_external_id(parent.id(), query_args)
        .await?;
    assert_eq!(ret.entities[0].external_id, Some(format!("z-{random}")));

    let query_args = es_entity::PaginatedQueryArgs {
        first: 1,
        after: Some(AccountSetMemberByExternalIdCursor::from(&ret.entities[0])),
    };
    let ret = cala
        .account_sets()
        .list_members_by_external_id(parent.id(), query_args)
        .await?;
    assert_eq!(ret.entities[0].external_id, None);

    Ok(())
}

#[tokio::test]
async fn list_eventually_consistent_ids() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let journal = cala
        .journals()
        .create(helpers::test_journal())
        .await
        .unwrap();

    let inline_set = NewAccountSet::builder()
        .id(AccountSetId::new())
        .name("Inline Set")
        .journal_id(journal.id())
        .balance_rollup(BalanceRollup::Synchronous)
        .build()
        .unwrap();
    let inline_set = cala.account_sets().create(inline_set).await.unwrap();

    let mut expected_ec_ids = Vec::new();
    for i in 0..3 {
        let ec_set = NewAccountSet::builder()
            .id(AccountSetId::new())
            .name(format!("EC Set {i}"))
            .journal_id(journal.id())
            .balance_rollup(BalanceRollup::EventuallyConsistent)
            .build()
            .unwrap();
        let ec_set = cala.account_sets().create(ec_set).await.unwrap();
        expected_ec_ids.push(ec_set.id());
    }

    // Walk the full list in pages of 2 and collect all returned ids in order.
    let mut collected = Vec::new();
    let mut after: Option<AccountSetByIdCursor> = None;
    loop {
        let ret = cala
            .account_sets()
            .list_eventually_consistent_ids(es_entity::PaginatedQueryArgs {
                first: 2,
                after: after.take(),
            })
            .await?;
        assert!(ret.entities.len() <= 2, "page should respect `first` limit");
        collected.extend(ret.entities);
        if !ret.has_next_page {
            break;
        }
        after = ret.end_cursor;
        assert!(
            after.is_some(),
            "next page requires an end cursor when has_next_page is true"
        );
    }

    // EC ids should come out sorted by id ascending across pages.
    let mut prev: Option<AccountSetId> = None;
    for id in &collected {
        if let Some(p) = prev {
            assert!(p < *id, "ids must be strictly ascending across pages");
        }
        prev = Some(*id);
    }

    for id in &expected_ec_ids {
        assert!(
            collected.contains(id),
            "eventually consistent set {id} should be listed across pages"
        );
    }
    assert!(
        !collected.contains(&inline_set.id()),
        "inline set should not be listed as eventually consistent"
    );

    Ok(())
}

/// `add_member` must reject candidates that already have any
/// `cala_balance_history` rows in the journal: the only way to honour
/// pre-existing balance would be to fold it synchronously, but that fold
/// races with concurrent posters of *other* members for EC sets and has
/// no race-free analogue we want to maintain for non-EC sets either.
#[tokio::test]
async fn add_member_errors_when_member_has_history() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let journal = cala
        .journals()
        .create(helpers::test_journal())
        .await
        .unwrap();

    let (sender, recipient) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await.unwrap();
    let recipient = cala.accounts().create(recipient).await.unwrap();

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await
        .unwrap();

    // Post once so the recipient has history before any membership change.
    let mut params = Params::new();
    params.insert("journal_id", journal.id().to_string());
    params.insert("sender", sender.id());
    params.insert("recipient", recipient.id());
    cala.post_transaction(TransactionId::new(), &tx_code, params)
        .await
        .unwrap();

    let target = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Target")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    let err = cala
        .account_sets()
        .add_member(target.id(), recipient.id())
        .await
        .err()
        .expect("add_member should fail when the member has balance history");

    match err {
        AccountSetError::MemberHasBalanceHistory {
            account_set_id,
            member_id,
        } => {
            assert_eq!(account_set_id, target.id());
            assert_eq!(member_id, recipient.id());
        }
        other => panic!("expected MemberHasBalanceHistory, got {other}"),
    }

    // Adding a fresh account with no history is still allowed.
    let fresh = cala
        .accounts()
        .create(
            NewAccount::builder()
                .id(uuid::Uuid::now_v7())
                .name("Fresh")
                .code(Alphanumeric.sample_string(&mut rand::rng(), 32))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    cala.account_sets()
        .add_member(target.id(), fresh.id())
        .await
        .unwrap();

    Ok(())
}

/// `remove_member` must reject members that have any
/// `cala_balance_history` rows: there is no safe way to back the member's
/// past contribution out of the parent set's running balance.
#[tokio::test]
async fn remove_member_errors_when_member_has_history() -> anyhow::Result<()> {
    let pool = helpers::init_pool().await?;
    let cala_config = CalaLedgerConfig::builder()
        .pool(pool)
        .exec_migrations(false)
        .build()?;
    let cala = CalaLedger::init(cala_config).await?;

    let journal = cala
        .journals()
        .create(helpers::test_journal())
        .await
        .unwrap();

    let (sender, recipient) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await.unwrap();
    let recipient = cala.accounts().create(recipient).await.unwrap();

    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await
        .unwrap();

    let target = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Target")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

    // Add the recipient *before* it has any history (allowed) — then post
    // to it so that subsequent removal becomes a forbidden operation.
    cala.account_sets()
        .add_member(target.id(), recipient.id())
        .await
        .unwrap();

    let mut params = Params::new();
    params.insert("journal_id", journal.id().to_string());
    params.insert("sender", sender.id());
    params.insert("recipient", recipient.id());
    cala.post_transaction(TransactionId::new(), &tx_code, params)
        .await
        .unwrap();

    let err = cala
        .account_sets()
        .remove_member(target.id(), recipient.id())
        .await
        .err()
        .expect("remove_member should fail when the member has balance history");

    match err {
        AccountSetError::MemberHasBalanceHistory {
            account_set_id,
            member_id,
        } => {
            assert_eq!(account_set_id, target.id());
            assert_eq!(member_id, recipient.id());
        }
        other => panic!("expected MemberHasBalanceHistory, got {other}"),
    }

    Ok(())
}
