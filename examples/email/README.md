# Email pipeline example

A runnable program that shows drover end to end: enqueue a batch of jobs,
work them concurrently on a pool of workers, retry the ones that fail
their first delivery attempt, and shut down cleanly on Ctrl-C.

## Running it

It needs a reachable PostgreSQL database — the program migrates the
schema itself, but something has to be listening. For example, with a
throwaway container:

```sh
docker run --rm -e POSTGRES_PASSWORD=drover -p 5432:5432 postgres:17
```

Then, from the repository root:

```sh
DATABASE_URL="postgres://postgres:drover@localhost:5432/postgres" \
  go run ./examples/email
```

## What to watch for

- **Enqueue line**: the program prints how many of the 20 enqueued
  emails belong to the deterministic "flaky" subset defined in
  `delivery.go` — those are the ones that will retry. The subset is a
  pure function of the recipient address (an FNV-1a hash mod 3), not
  randomness, so it is the same on every run: `user1@example.com` always
  fails its first attempt, `user5@example.com` never does.
- **Concurrent delivery**: with `Concurrency: 4`, delivered lines for
  different recipients interleave rather than appearing one at a time.
- **Retries**: a flaky recipient's job fails once, drover schedules a
  retry roughly a second later (`ExponentialRetryPolicy`'s attempt¹
  wait), and a `delivered ... attempt 2` line follows shortly after the
  matching failure log.
- **Shutdown**: press Ctrl-C. The program stops claiming new jobs
  immediately, waits up to 30 seconds for whatever is already running to
  finish, and prints either that everything drained or, if the budget
  ran out, how many jobs did not finish and were returned to the queue.

## No external dependencies

The example uses only the standard library plus what the drover module
already requires (`pgx`); it adds no new entry to `go.mod`.
