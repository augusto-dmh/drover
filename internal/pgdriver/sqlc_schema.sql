-- Session-temp staging table used by InsertMany. Declared here so sqlc
-- can type-check InsertJobsFromStaging; the live table is created by
-- pgdriver with CREATE TEMP TABLE ... ON COMMIT DROP, not by a migration.
CREATE TABLE drover_insert_batch (
    ord int NOT NULL,
    kind text NOT NULL,
    queue text NOT NULL,
    args jsonb NOT NULL,
    scheduled_at timestamptz,
    unique_key text
);
