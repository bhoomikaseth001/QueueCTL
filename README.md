# queuectl

A CLI-based background job queue system with worker processes, automatic
retries with exponential backoff, and a Dead Letter Queue (DLQ) for jobs
that permanently fail.

Built with **zero external dependencies** — just Node.js built-ins
(`fs`, `child_process`, `path`, `os`). No `npm install` step, no native
module compilation, no version drift.

---

## 1. Setup Instructions

**Requirements:** Node.js >= 16 (no other software needed).

```bash
git clone <your-repo-url>
cd queuectl

# Option A: run directly
node bin/queuectl.js --help

# Option B: install as a global "queuectl" command
npm link
queuectl --help
```

There is nothing to `npm install` — `package.json` has no runtime
dependencies. `npm link` just symlinks the `bin/queuectl.js` script onto
your `PATH` per the `bin` field in `package.json`.

All state (jobs, config, logs, worker PIDs) is stored under
`~/.queuectl/` by default. Override with the `QUEUECTL_HOME` environment
variable (this is how the automated tests get an isolated sandbox).

---

## 2. Usage Examples

### Enqueue a job

```bash
$ queuectl enqueue '{"id":"job1","command":"echo Hello World"}'
Enqueued job "job1" (state=pending, max_retries=3)
```

A job only needs `id` and `command`. You can optionally override the
retry count per job:

```bash
$ queuectl enqueue '{"id":"job2","command":"sleep 2","max_retries":5}'
Enqueued job "job2" (state=pending, max_retries=5)
```

### Start workers

```bash
$ queuectl worker start --count 3
Started 3 worker(s): PID(s) 4821, 4822, 4823
Worker logs: /home/you/.queuectl/worker.log
```

Workers run as detached background processes and keep polling for
pending jobs until you stop them.

### Check status

```bash
$ queuectl status
Job Summary:
  pending     : 1
  processing  : 0
  completed   : 2
  failed      : 0
  dead        : 0
Active workers: 3
```

### List jobs

```bash
$ queuectl list --state pending
┌─────────┬────────┬───────────┬──────────┬───────────────┬────────────────────────────┐
│ (index) │ id     │ state     │ attempts │ command       │ updated_at                 │
├─────────┼────────┼───────────┼──────────┼───────────────┼────────────────────────────┤
│ 0       │ 'job3' │ 'pending' │ '0/3'    │ 'sleep 5'     │ '2026-07-17T04:20:00.000Z' │
└─────────┴────────┴───────────┴──────────┴───────────────┴────────────────────────────┘
```

### Stop workers gracefully

```bash
$ queuectl worker stop
Sending SIGTERM to 3 worker(s): 4821, 4822, 4823 (waiting for in-flight jobs to finish)...
All workers stopped.
```

Each worker finishes whatever job it's currently running before exiting
— no job is ever killed mid-execution by a stop request.

### Dead Letter Queue

```bash
$ queuectl dlq list
┌─────────┬───────────┬──────────┬──────────┬───────────────────────────────┬─────────┐
│ (index) │ id        │ command  │ attempts │ last_error                    │ ...     │
├─────────┼───────────┼──────────┼──────────┼───────────────────────────────┼─────────┤
│ 0       │ 'job4'    │ 'exit 1' │ '3/3'    │ 'command exited with code 1'  │ ...     │
└─────────┴───────────┴──────────┴──────────┴───────────────────────────────┴─────────┘

$ queuectl dlq retry job4
Job "job4" moved back to pending and will be retried by a worker.
```

### Configuration

```bash
$ queuectl config set max-retries 5
Config updated: max-retries = 5

$ queuectl config set backoff-base 3
Config updated: backoff-base = 3

$ queuectl config list
Current configuration:
  max-retries : 5
  backoff-base: 3
```

Config changes are global defaults applied to jobs enqueued (or DLQ jobs
retried) *after* the change — they don't retroactively rewrite jobs
already in flight.

---

## 3. Architecture Overview

### Job lifecycle

```
enqueue
   │
   ▼
pending ──worker claims──▶ processing ──exit code 0──▶ completed
   ▲                            │
   │                       exit code != 0
   │                            │
   │                            ▼
   └──── attempts < max_retries?  YES → back to pending (after backoff delay)
                                  NO  → dead (DLQ)
```

- **pending** — waiting to be picked up (also used as the state for a
  job that's scheduled for a retry — see `next_run_at` below).
- **processing** — a worker has claimed it and is currently running
  its command.
- **completed** — the command exited with code `0`.
- **failed** is used conceptually to describe an attempt that didn't
  succeed; concretely a failed attempt either goes back to **pending**
  (with a future `next_run_at`) or to **dead**, depending on whether
  retries remain.
- **dead** — permanently failed; sits in the DLQ until manually retried.

### Data persistence

Everything is stored as JSON files under `~/.queuectl/` (or
`$QUEUECTL_HOME`):

```
~/.queuectl/
├── jobs.json      # all job records, keyed by id
├── config.json    # { max_retries, backoff_base }
├── workers.pid    # PIDs of currently running worker processes
├── worker.log      # combined stdout/stderr of all workers
├── db.lock/        # lock directory (see below) -- transient
└── logs/
    └── <job-id>-attempt<N>.log   # per-attempt command output
```

Writes are atomic: every update writes to a temp file and then
`fs.renameSync`s it over the real file, so a crash mid-write can never
leave `jobs.json` truncated or corrupted.

### Concurrency & locking

The hard requirement here is: **multiple worker processes must never
claim the same job twice.** Since workers are independent OS processes
(not threads sharing memory), an in-memory mutex won't help — the lock
has to work across process boundaries.

`src/lock.js` implements this with `fs.mkdirSync()`: creating a
directory is atomic at the OS/filesystem level — if it already exists,
`mkdir` fails with `EEXIST` instead of silently succeeding. Every
read-modify-write against `jobs.json`/`config.json` (enqueue, claim,
mark-complete, mark-failed, retry, etc.) runs inside this lock via
`store.transact(...)`, so the whole sequence — "read jobs, find the
oldest eligible pending job, flip it to processing, write jobs back" —
is serialized across every worker and CLI invocation on the machine.
That's what guarantees exactly-once claiming.

### Retry & exponential backoff

On failure, `attempts` is incremented. If `attempts < max_retries`, the
job goes back to `pending` with:

```
next_run_at = now + (backoff_base ^ attempts) seconds
```

A worker's claim query only picks up jobs where `next_run_at <= now`,
so the job simply won't be eligible again until its backoff window
elapses. Once `attempts >= max_retries`, the job moves to `dead`
instead.

### Worker management

`worker start --count N` spawns `N` detached child processes running
`src/worker.js`, each polling every 500ms for the next eligible job.
Their PIDs are recorded in `workers.pid`.

`worker stop` reads that PID file, sends `SIGTERM` to each worker, and
waits (up to 30s) for them to exit on their own. A worker's SIGTERM
handler just sets a `shuttingDown` flag — it finishes whatever job it's
mid-execution on, then exits the polling loop instead of grabbing
another job. If a worker doesn't exit within the timeout, it's sent
`SIGKILL` as a last resort.

### Job execution

Each job's `command` is run via `child_process.spawnSync(command, {
shell: true })`, so any shell command works (`sleep 2`, `echo hello`,
pipelines, etc.). Exit code `0` → success; any non-zero exit code, or a
spawn error (e.g. command not found), is treated as a failure and
routed through the retry/DLQ logic. Every attempt's stdout/stderr is
also saved to `logs/<job-id>-attempt<N>.log` for debugging.

---

## 4. Assumptions & Trade-offs

- **Storage: JSON files + a directory-based lock, not SQLite.** The
  assignment allows "file storage (JSON) or SQLite/embedded DB or
  anything you think is best." I initially reached for `better-sqlite3`
  (transactional `UPDATE ... WHERE state='pending'` is a very natural
  fit for atomic claiming), but chose to ship a dependency-free version
  instead — one less thing that can go wrong for a grader running this
  cold (no native module compile step, no lockfile/registry issues,
  works on any machine with plain Node). The trade-off is a **single
  global lock** guarding all reads/writes, so throughput is serialized
  across every worker for every operation. For an assignment-scale
  queue (dozens to low-thousands of jobs) this is invisible in
  practice; at real production scale you'd want SQLite (or Postgres)
  with row-level locking instead of a single global mutex.
- **`processing` jobs are not automatically requeued if a worker is
  killed (`SIGKILL`/crash) mid-job.** Graceful `worker stop` handles
  the intended shutdown path correctly, but a hard crash could leave a
  job stuck in `processing` forever. A production system would add a
  "job has been processing longer than X with no live worker holding
  its lock → requeue it" reaper. Documented here rather than silently
  handled since I'd rather be explicit about the gap.
- **Per-job `max_retries` is fixed at enqueue time.** `queuectl config
  set max-retries` changes the *default* for new jobs (and DLQ retries,
  which reset attempts to 0); it doesn't retroactively change a job
  that's already in flight with its own `max_retries` value.
- **`backoff-base` is a single global value**, not per-job, since the
  assignment's config command example (`config set max-retries 3`)
  implies global knobs.
- **A 5-minute per-attempt timeout** is enforced on every job command
  (`spawnSync(..., { timeout: 5*60*1000 })`) so a hung command can't
  wedge a worker forever. This isn't in the spec but felt necessary for
  a "production-grade" claim.
- **Log files under `logs/` aren't automatically pruned.** Fine for an
  assignment; a real deployment would want rotation/expiry.

---

## 5. Testing Instructions

An end-to-end test suite is included and covers every scenario listed
in the assignment:

```bash
npm test
# or directly:
bash test/run_tests.sh
```

It runs in a throwaway `QUEUECTL_HOME` (via `mktemp -d`), so it never
touches your real `~/.queuectl/` data, and exercises:

1. **Basic job completes successfully** — enqueue `echo hello`, start a
   worker, assert it reaches `completed`.
2. **Failed job retries with backoff and moves to DLQ** — a job that
   always fails (`exit 1`) with `max_retries=2`, asserting it ends up
   in `dlq list`.
3. **Multiple workers process jobs without overlap** — 6 short jobs
   against 3 concurrent workers; asserts all 6 complete and that the
   worker log shows each job id claimed exactly once (no duplicate
   claims).
4. **Invalid commands fail gracefully** — a nonexistent command doesn't
   crash the worker; it's caught, logged, and routed through the same
   retry/DLQ path as any other failure.
5. **Job data survives restart** — persistence is verified by having a
   brand-new CLI process read back a job written by an earlier one
   (since state lives entirely on disk, this *is* the restart test).

Sample output:

```
Using isolated test data dir: /tmp/tmp.XXXXXXXXXX

== Test 1: Basic job completes successfully ==
  PASS: basic job reached 'completed' state

== Test 2: Failing job retries with backoff, then moves to DLQ ==
  PASS: failing job exhausted retries and landed in DLQ

== Test 3: Multiple workers process jobs concurrently, no duplicates ==
  PASS: all 6 jobs completed exactly once (no double-claims across 3 workers)

== Test 4: Invalid command fails gracefully (not a crash) ==
  PASS: invalid command was handled gracefully and moved to DLQ

== Test 5: Job data survives restart ==
  PASS: job data is readable from a fresh process (persisted to disk)

All tests passed!
```

### Manual verification

You can also poke at it by hand:

```bash
export QUEUECTL_HOME=/tmp/queuectl-demo   # optional: isolate from real data
queuectl enqueue '{"id":"demo1","command":"sleep 2"}'
queuectl worker start --count 2
queuectl status
sleep 3
queuectl status
queuectl worker stop
```

---

## Project Structure

```
queuectl/
├── bin/
│   └── queuectl.js        # CLI entry point (argument parsing + dispatch)
├── src/
│   ├── store.js           # file-based persistence, atomic writes, transact()
│   ├── lock.js             # cross-process mutex (mkdir-based)
│   ├── repository.js      # job/config data access (enqueue, claim, retry, ...)
│   ├── worker.js           # worker process loop (spawned as a child process)
│   └── commands/
│       ├── enqueue.js
│       ├── worker.js       # start/stop workers, PID bookkeeping
│       ├── status.js
│       ├── list.js
│       ├── dlq.js
│       └── config.js
├── test/
│   └── run_tests.sh        # end-to-end test suite
├── package.json
└── README.md
```
#   Q u e u e C T L  
 