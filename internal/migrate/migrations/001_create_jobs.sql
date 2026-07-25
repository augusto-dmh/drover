CREATE TYPE drover_job_state AS ENUM (
  'available',
  'scheduled',
  'running',
  'retryable',
  'completed',
  'cancelled',
  'dead'
);

CREATE TABLE drover_jobs (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind         text NOT NULL,
  queue        text NOT NULL DEFAULT 'default',
  args         jsonb NOT NULL DEFAULT '{}',
  state        drover_job_state NOT NULL DEFAULT 'available',
  attempt      int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 25,
  errors       jsonb NOT NULL DEFAULT '[]',
  scheduled_at timestamptz NOT NULL DEFAULT now(),
  leased_until timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  finalized_at timestamptz
);

CREATE INDEX drover_jobs_fetch_idx
  ON drover_jobs (queue, scheduled_at, id)
  WHERE state = 'available';
