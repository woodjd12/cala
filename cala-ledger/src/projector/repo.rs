use job::JobId;

use super::{error::ProjectorError, ProjectorState};

pub(super) struct ProjectorRepo;

impl ProjectorRepo {
    /// Advance the persisted cursor with a compare-and-swap; an instance
    /// that loses the CAS aborts its whole operation, snapshots included.
    /// The COALESCE covers the NULL initial state, the `::BIGINT` cast is
    /// required because `->>` yields text, and the CAS stays the last
    /// statement of the batch so the row lock is not held while the job
    /// poller's keep-alive updates the same row.
    pub(super) async fn advance_cursor_in_op(
        op: &mut impl es_entity::AtomicOperation,
        job_id: JobId,
        expected: i64,
        new_state: &ProjectorState,
    ) -> Result<(), ProjectorError> {
        let execution_state_json = serde_json::to_value(new_state)?;
        let result = sqlx::query(
            r#"
            UPDATE job_executions
            SET execution_state_json = $1
            WHERE id = $2
            AND COALESCE((execution_state_json->>'sequence')::BIGINT, 0) = $3
            "#,
        )
        .bind(execution_state_json)
        .bind(uuid::Uuid::from(job_id))
        .bind(expected)
        .execute(op.as_executor())
        .await?;
        match result.rows_affected() {
            1 => Ok(()),
            0 => Err(ProjectorError::CursorFenced { job_id, expected }),
            n => unreachable!("job_executions.id is unique but the cursor CAS matched {n} rows"),
        }
    }
}
