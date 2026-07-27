use thiserror::Error;

use job::JobId;

#[derive(Error, Debug)]
pub enum ProjectorError {
    #[error("ProjectorError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("ProjectorError - Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("ProjectorError - AccountSetError: {0}")]
    AccountSetError(#[from] crate::account_set::error::AccountSetError),
    #[error("ProjectorError - BalanceError: {0}")]
    BalanceError(#[from] crate::balance::error::BalanceError),
    #[error("ProjectorError - JournalError: {0}")]
    JournalError(#[from] crate::journal::error::JournalError),
    #[error("ProjectorError - TransactionError: {0}")]
    TransactionError(#[from] crate::transaction::error::TransactionError),
    #[error(
        "ProjectorError - CursorFenced: job '{job_id}' expected cursor at \
         sequence {expected} but another instance has advanced it"
    )]
    CursorFenced { job_id: JobId, expected: i64 },
}
