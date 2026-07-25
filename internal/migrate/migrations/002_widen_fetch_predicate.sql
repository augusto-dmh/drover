-- A job waits for execution in one of three states: available (never
-- run), retryable (failed, waiting out its backoff) or scheduled
-- (snoozed). All three are claimed by the same fetch, so the partial
-- index that serves it must cover all three.
DROP INDEX drover_jobs_fetch_idx;

CREATE INDEX drover_jobs_fetch_idx
  ON drover_jobs (queue, scheduled_at, id)
  WHERE state IN ('available', 'retryable', 'scheduled');

-- The rescuer sweeps for running jobs whose lease has passed.
CREATE INDEX drover_jobs_lease_idx
  ON drover_jobs (leased_until)
  WHERE state = 'running';
