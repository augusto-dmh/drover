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
    WHERE j.state = 'available'
      AND j.queue = sqlc.arg(queue)
      AND j.scheduled_at <= now()
    ORDER BY j.id
    LIMIT sqlc.arg(max_jobs)
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: GetJob :one
SELECT * FROM drover_jobs WHERE id = $1;

-- name: MarkCompleted :execrows
UPDATE drover_jobs
SET state = 'completed', finalized_at = now()
WHERE id = $1 AND state = 'running';

-- name: MarkDead :execrows
UPDATE drover_jobs
SET state = 'dead',
    finalized_at = now(),
    errors = errors || sqlc.arg(error)::jsonb
WHERE id = $1 AND state = 'running';
