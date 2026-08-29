-- A null scheduled_at means "now". The state follows from the same
-- comparison the fetch predicate makes, evaluated by the database rather
-- than the client: a job due later waits in scheduled, one due now is
-- available. Deciding this on the client would let a machine running
-- fast or slow write a state this database disagrees with, and the state
-- column is what an operator is shown.
--
-- now() is transaction_timestamp(), so both readings here are the same
-- instant and the stored time can never disagree with the state derived
-- from it. On the InsertTx path that instant is the caller's transaction
-- start, not the insert, so a job enqueued late in a long transaction
-- can be recorded scheduled while already due. Nothing is delayed by it:
-- the fetch predicate compares scheduled_at against its own now() and
-- claims the row on the next poll. clock_timestamp() would fix the label
-- and break the guarantee — being volatile, its two occurrences would
-- evaluate at different instants and could disagree for real.
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

-- Same CASE as InsertJob: dueness is the database clock's, not the
-- client's. Rows are taken from the session-temp staging table in the
-- order CopyFrom loaded them.
-- name: InsertJobsFromStaging :many
INSERT INTO drover_jobs (kind, queue, args, scheduled_at, state)
SELECT
    kind,
    queue,
    args,
    coalesce(scheduled_at, now()),
    CASE WHEN coalesce(scheduled_at, now()) > now()
         THEN 'scheduled'::drover_job_state
         ELSE 'available'::drover_job_state
    END
FROM drover_insert_batch
ORDER BY ord
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

-- Empty queue or state means no filter. Newest id first so a bounded
-- listing is stable without a new index, and operators see recent work
-- first. Limit is the caller's responsibility to keep positive.
-- name: ListJobs :many
SELECT * FROM drover_jobs
WHERE (sqlc.arg(queue) = '' OR queue = sqlc.arg(queue))
  AND (sqlc.arg(state)::text = '' OR state = sqlc.arg(state)::drover_job_state)
ORDER BY id DESC
LIMIT sqlc.arg(max_jobs);

-- Operator cancel is state-conditioned, not lease-fenced: it must not
-- invent a fake attempt or stomp a running worker. Zero rows means the
-- caller must re-read to distinguish missing from wrong state.
-- name: OperatorCancel :one
UPDATE drover_jobs
SET state = 'cancelled',
    finalized_at = now(),
    leased_until = NULL
WHERE id = sqlc.arg(id)
  AND state IN ('available', 'scheduled', 'retryable', 'dead')
RETURNING *;

-- Redrive resets attempt and the lease, keeps error history for triage,
-- and uses the database clock for scheduled_at so the job is claimable
-- immediately against the same clock the fetch predicate uses.
-- name: RedriveDead :one
UPDATE drover_jobs
SET state = 'available',
    attempt = 0,
    leased_until = NULL,
    finalized_at = NULL,
    scheduled_at = now()
WHERE id = sqlc.arg(id)
  AND state = 'dead'
RETURNING *;

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

-- Only the states an operator can act on are counted. completed and
-- cancelled are deliberately absent: those rows are never removed, so
-- counting them would make this query's cost grow with the whole history
-- of the queue rather than with its backlog — turning the thing that
-- reports load into a source of it. Every state listed here is served by
-- a partial index: the three waiting ones by the fetch index, running by
-- the lease index, dead by its own.
--
-- The cast to text is what keeps the enum out of the driver: without it
-- the generated row would carry a generated enum type, and the state
-- would have to be translated again on its way to a metric label.
-- name: QueueDepths :many
SELECT queue, state::text AS state, count(*) AS count
FROM drover_jobs
WHERE state IN ('available', 'scheduled', 'retryable', 'running', 'dead')
GROUP BY queue, state;

-- The predicate is the fetch predicate, deliberately: "oldest job" has to
-- mean "oldest job that should already have run", or a job scheduled for
-- next week would report a week of lateness the moment it is enqueued and
-- every delayed job would look like an outage. Anything that widens what
-- FetchAvailable claims has to widen this too, or the age stops measuring
-- what the claim round is actually behind on.
--
-- The age is subtracted here rather than returned as a timestamp for the
-- caller to compare against its own clock: the scheduled time is decided
-- by this database's clock, so only this database can say how late it is.
-- A client running fast would otherwise publish an age no other client
-- agrees with.
-- name: OldestClaimable :many
SELECT queue,
       EXTRACT(EPOCH FROM (now() - min(scheduled_at)))::float8 AS age_seconds
FROM drover_jobs
WHERE state IN ('available', 'retryable', 'scheduled')
  AND scheduled_at <= now()
GROUP BY queue;
