// Package drover is a PostgreSQL-backed task queue: typed jobs are
// enqueued transactionally and executed by registered workers.
//
// Delivery contract and operational notes are completed alongside the
// worker loop; see the repository README and docs/adr for design
// rationale.
package drover
