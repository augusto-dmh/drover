-- name: InsertJob :one
INSERT INTO drover_jobs (kind, queue, args)
VALUES ($1, $2, $3)
RETURNING *;

-- name: FetchAvailable :many
UPDATE drover_jobs
SET state = 'running',
    attempt = attempt + 1,
    leased_until = sqlc.arg(leased_until)::timestamptz
WHERE id IN (
    SELECT j.id FROM drover_jobs j
    WHERE j.state IN ('available', 'retryable', 'scheduled')
      AND j.queue = sqlc.arg(queue)
      AND j.scheduled_at <= now()
    ORDER BY j.id
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
SET leased_until = sqlc.arg(leased_until)::timestamptz
WHERE id IN (
    SELECT j.id FROM drover_jobs j
    WHERE j.state = 'running'
      AND j.leased_until <= now()
    ORDER BY j.id
    LIMIT sqlc.arg(max_jobs)
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: ExtendLeases :exec
UPDATE drover_jobs
SET leased_until = sqlc.arg(leased_until)::timestamptz
WHERE id = ANY(sqlc.arg(ids)::bigint[])
  AND state = 'running';

-- name: GetJob :one
SELECT * FROM drover_jobs WHERE id = $1;

-- name: MarkCompleted :execrows
UPDATE drover_jobs
SET state = 'completed',
    finalized_at = now(),
    leased_until = NULL
WHERE id = $1 AND state = 'running';

-- name: MarkRetryable :execrows
UPDATE drover_jobs
SET state = 'retryable',
    scheduled_at = sqlc.arg(retry_at)::timestamptz,
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = sqlc.arg(id) AND state = 'running';

-- name: MarkDead :execrows
UPDATE drover_jobs
SET state = 'dead',
    finalized_at = now(),
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = $1 AND state = 'running';

-- name: MarkCancelled :execrows
UPDATE drover_jobs
SET state = 'cancelled',
    finalized_at = now(),
    leased_until = NULL,
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = sqlc.arg(id) AND state = 'running';

-- MarkSnoozed gives back the attempt the claim consumed, floored at zero
-- so a deferral can never drive attempt negative or exhaust a job.
-- name: MarkSnoozed :execrows
UPDATE drover_jobs
SET state = 'scheduled',
    scheduled_at = sqlc.arg(run_at)::timestamptz,
    leased_until = NULL,
    attempt = GREATEST(attempt - 1, 0)
WHERE id = sqlc.arg(id) AND state = 'running';
