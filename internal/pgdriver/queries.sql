-- A null scheduled_at means "now". The state follows from the same
-- comparison the fetch predicate makes, evaluated against the same
-- clock: a job due later waits in scheduled, one due now is available.
-- Deciding this on the client would let a machine running fast or slow
-- write a state this database disagrees with, and the state column is
-- what an operator is shown.
-- name: InsertJob :one
INSERT INTO drover_jobs (kind, queue, args, scheduled_at, state)
VALUES (
    $1, $2, $3,
    coalesce(sqlc.narg(scheduled_at)::timestamptz, now()),
    CASE WHEN coalesce(sqlc.narg(scheduled_at)::timestamptz, now()) > now()
         THEN 'scheduled'::drover_job_state
         ELSE 'available'::drover_job_state
    END
)
RETURNING *;

-- Lease deadlines are computed here, from the database clock, because
-- that is the clock the sweep compares them against. Deriving them on
-- the client would make every lease depend on two machines agreeing.
-- name: FetchAvailable :many
UPDATE drover_jobs
SET state = 'running',
    attempt = attempt + 1,
    leased_until = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
WHERE id IN (
    SELECT j.id FROM drover_jobs j
    WHERE j.state IN ('available', 'retryable', 'scheduled')
      AND j.queue = sqlc.arg(queue)
      AND j.scheduled_at <= now()
    -- Ordered by the index's own key, not by id: with queue as an
    -- equality and scheduled_at as a range, the index yields rows in
    -- (scheduled_at, id) order, so ordering by id alone would force
    -- every due row to be read and sorted before LIMIT could take one.
    -- Due-time order is also the more correct claim order — a retry that
    -- came due first should be picked up first.
    ORDER BY j.scheduled_at, j.id
    LIMIT sqlc.arg(max_jobs)
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- FetchExpired re-claims jobs whose worker is presumed dead. attempt is
-- deliberately not incremented: the attempt that stranded the row was
-- really spent, and counting it twice would halve the effective ceiling
-- of every job whose worker ever crashed.
-- name: FetchExpired :many
UPDATE drover_jobs
SET leased_until = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
WHERE id IN (
    SELECT j.id FROM drover_jobs j
    WHERE j.state = 'running'
      AND j.leased_until <= now()
    -- Oldest lease first, matching the lease index so the batch is taken
    -- without sorting every abandoned row — and recovering the
    -- longest-stranded jobs first.
    ORDER BY j.leased_until, j.id
    LIMIT sqlc.arg(max_jobs)
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: ExtendLeases :exec
UPDATE drover_jobs j
SET leased_until = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8)
FROM (
    SELECT unnest(sqlc.arg(ids)::bigint[]) AS id,
           unnest(sqlc.arg(attempts)::int[]) AS attempt
) AS held
WHERE j.id = held.id
  AND j.attempt = held.attempt
  AND j.state = 'running';

-- name: GetJob :one
SELECT * FROM drover_jobs WHERE id = $1;

-- Every finalizer is guarded on the attempt as well as the state. The
-- state alone proves some worker holds the row, not that this one does:
-- a worker whose heartbeat starved past its lease can find its job
-- rescued and re-claimed, and must not then record the outcome of an
-- attempt it no longer owns over the one now running.
-- name: MarkCompleted :execrows
UPDATE drover_jobs
SET state = 'completed',
    finalized_at = now(),
    leased_until = NULL
WHERE id = sqlc.arg(id) AND attempt = sqlc.arg(attempt) AND state = 'running';

-- name: MarkRetryable :execrows
UPDATE drover_jobs
SET state = 'retryable',
    scheduled_at = sqlc.arg(retry_at)::timestamptz,
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = sqlc.arg(id) AND attempt = sqlc.arg(attempt) AND state = 'running';

-- name: MarkDead :execrows
UPDATE drover_jobs
SET state = 'dead',
    finalized_at = now(),
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = sqlc.arg(id) AND attempt = sqlc.arg(attempt) AND state = 'running';

-- name: MarkCancelled :execrows
UPDATE drover_jobs
SET state = 'cancelled',
    finalized_at = now(),
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = sqlc.arg(id) AND attempt = sqlc.arg(attempt) AND state = 'running';

-- MarkSnoozed gives back the attempt the claim consumed, floored at zero
-- so a deferral can never drive attempt negative or exhaust a job.
-- name: MarkSnoozed :execrows
UPDATE drover_jobs
SET state = 'scheduled',
    scheduled_at = sqlc.arg(run_at)::timestamptz,
    leased_until = NULL,
    attempt = GREATEST(attempt - 1, 0)
WHERE id = sqlc.arg(id) AND attempt = sqlc.arg(attempt) AND state = 'running';
