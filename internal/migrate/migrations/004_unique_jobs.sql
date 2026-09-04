-- Unique jobs occupy at most one non-terminal row per
-- (queue, kind, unique_key). Empty unique_key is stored NULL and is
-- excluded from the index, so jobs that are not unique never collide
-- with each other. Terminal states (completed, cancelled, dead) drop
-- out of the index so a legitimate re-enqueue can reuse the key.
ALTER TABLE drover_jobs ADD COLUMN unique_key text;

CREATE UNIQUE INDEX drover_jobs_unique_active_idx
  ON drover_jobs (queue, kind, unique_key)
  WHERE unique_key IS NOT NULL
    AND state IN ('available', 'scheduled', 'retryable', 'running');
