-- The metrics refresher counts jobs per queue and state on an interval.
-- Dead is the one state it counts that no existing index covers: the
-- fetch index serves the three waiting states and the lease index serves
-- running, so without this one that count degrades to a sequential scan
-- over the whole table — a cost that grows with the completed-job
-- history, which nothing prunes. That would make the component whose
-- purpose is to report load into a source of it.
--
-- Built with a plain CREATE INDEX rather than CONCURRENTLY because each
-- migration runs inside one transaction and CONCURRENTLY cannot. The
-- SHARE lock it takes blocks writes for the length of the build, which
-- here is a build over only the rows that have died — by construction the
-- rare path, and the same reason the index is close to free to maintain
-- afterwards.
--
-- Keyed (queue, id) rather than (queue) alone so the count is answered
-- from the index without visiting the heap.
CREATE INDEX drover_jobs_dead_idx
  ON drover_jobs (queue, id)
  WHERE state = 'dead';
