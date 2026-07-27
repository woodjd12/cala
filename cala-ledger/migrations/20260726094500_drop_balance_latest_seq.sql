-- `cala_current_balances.latest_seq` was the pull-based recalculation's
-- watermark: "member history up to this seq has been folded into the set
-- balance". With recalculation replaced by the streaming balance projector,
-- the projector's outbox cursor (job_executions.execution_state_json) is the
-- only progress authority, and nothing reads this column.
--
-- `cala_balance_history.seq` is unaffected: it remains the ordering key for
-- the effective-balance rebuild.
ALTER TABLE cala_current_balances DROP COLUMN latest_seq;
