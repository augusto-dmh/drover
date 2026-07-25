// Package drover is a PostgreSQL-backed task queue: typed jobs are
// enqueued transactionally and executed by registered workers.
//
// # Delivery contract
//
// Drover delivers each job at least once. A job claimed by a worker
// that then crashes may be delivered again once lease-based rescue
// ships; duplicates are therefore possible by design and Worker
// implementations must be idempotent. Exactly-once execution is not
// promised by drover or by any queue — see docs/adr/0003 in the
// repository for the full reasoning.
//
// # Enqueueing
//
// Client.Insert persists a job in its own transaction. Client.InsertTx
// persists it inside a caller-owned pgx.Tx, so a domain write and its
// job commit or roll back atomically — no outbox required.
//
// # Current limitations
//
// This version runs a single worker goroutine per Start call and
// finalizes failed jobs as dead immediately: retries with backoff,
// lease enforcement with rescue of crashed workers' jobs, and pooled
// concurrency arrive in the next cycles of docs/rfc/0001 in the
// repository. A worker killed mid-job leaves the row in the running
// state until rescue lands; clean shutdown (context cancellation)
// always drains the in-flight job first.
package drover
