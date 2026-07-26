-- A job waits for execution in one of three states: available (never
-- run), retryable (failed, waiting out its backoff) or scheduled
-- (snoozed). All three are claimed by the same fetch, so the partial
-- index that serves it must cover all three.
--
-- The replacement is built under a new name and the old index dropped
-- last. Each migration runs in one transaction, and DROP INDEX takes an
-- ACCESS EXCLUSIVE lock on drover_jobs that is held until commit — so
-- dropping first would stall every enqueue and claim for the whole
-- length of the index build, on a table clients migrate at startup.
-- Building first leaves the drop as the only exclusive statement, and
-- the fetch path is never left without an index.
CREATE INDEX drover_jobs_fetch_v2_idx
  ON drover_jobs (queue, scheduled_at, id)
  WHERE state IN ('available', 'retryable', 'scheduled');

-- IF EXISTS because an operator may already have dropped or renamed the
-- original while tuning; that is expected work on this table, and it
-- must not make the migration permanently unappliable.
DROP INDEX IF EXISTS drover_jobs_fetch_idx;

-- The rescuer sweeps for running jobs whose lease has passed, oldest
-- lease first. Carrying id in the index lets that ordering be served
-- from the index alone, so a sweep after a multi-node crash takes its
-- batch without sorting the whole abandoned set.
CREATE INDEX drover_jobs_lease_idx
  ON drover_jobs (leased_until, id)
  WHERE state = 'running';
