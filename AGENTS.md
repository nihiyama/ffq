# AGENTS.md

##  Project Overview
FFQ (File-Based FIFO Queue) is a **file-based FIFO queue library for Go**.
- Designed for **high performance**: throughput is dominated by **serialization + I/O**; SPSC can be completely lock-free.
- **Standard library only** (no external dependencies) to keep the project lightweight and easy to vendor/use.
- Supports **generics**: you can enqueue any type.
- Provides two main queue types:
  - **SimpleQueue**: a single FIFO queue
  - **GroupQueue**: a collection of SimpleQueues keyed by a queue ID for multi-tenant / multi-stream usage
- Reliability: because queue data is stored on disk, the queue can be resumed after restart without losing dequeue progress (when index is correctly maintained).

Key operational rule (very important for agents changing code):
- Consumers should typically start the dequeue loop first, then call `WaitInitialize()`, then start producers.
- After each successful dequeue, call `UpdateIndex(...)` to persist progress.
- When the queue is closed, call `CloseIndex()` before exiting the consumer goroutine.

## Repository Structure

```
.
├── .devcontainer/            # Dev Container settings (reproducible dev env)
├── .github/workflows/        # CI workflows
├── .tasks/                   # Task-related configs/scripts (if used)
├── assets/                   # Images / logos
├── examples/                 # Usage examples
├── README.md                 # Library usage and notes
├── LICENSE
├── Taskfile.yaml             # Task runner entry (if using go-task)
├── go.mod / go.sum
├── *_queue.go                # Core queue implementations (SimpleQueue / GroupQueue)
├── options.go                # Options & configuration APIs
├── fsync.go                  # fsync-related behavior
├── pool.go                   # pooling utilities (perf)
├── utils.go / message.go     # supporting types/utilities
└── *_test.go                 # unit tests + benchmark_test.go
```

## ​ Setup & Build
- Requirements:
  - Go with generics support (Go 1.21+).
- Typical local workflow:
  - Download deps: `go mod download`
  - Run tests with race detector (when available): `task go:test`
  - Run benchmarks: `task go:bench`
- Optional:
  - If you use `Taskfile.yaml`, you may run `task -l` to list tasks, then run the relevant task(s).

##  Code Conventions
Follow the repository’s existing style and constraints:

- **No unnecessary dependencies**
  - FFQ’s stated direction is “keep it simple/lightweight”; avoid adding new third-party deps unless clearly justified.
- **API & behavior stability**
  - Preserve existing exported names and semantics unless the change explicitly requires it.
  - Be careful with any changes that affect **on-disk format** or **queue directory layout**.
- **Concurrency model is part of the contract**
  - Respect `QueueType` (e.g., SPSC vs MPSC). Avoid introducing data races or hidden locks in hot paths.
- **Index update semantics**
  - Typical usage pattern is:
    - start dequeue loop
    - call `WaitInitialize()` before starting enqueue
    - after each successful dequeue, call `UpdateIndex(...)`
    - when queue is closed, `CloseIndex()` and exit the consumer loop
  - Changes must not break this lifecycle.
- **Performance hygiene**
  - Avoid extra allocations in hot code paths.
  - Reuse buffers/pools when the existing implementation already does so.
  - Prefer straightforward code over cleverness unless benchmarks prove it’s needed.
- **Errors**
  - Use existing error helpers/patterns (e.g., “queue closed” handling) consistently.

##  Testing Instructions
Minimum bar for any behavioral change:

- Unit tests (and race):
  - `task go:test`
- Benchmarks (required if touching enqueue/dequeue, serialization, fsync, pooling, or file IO):
  - `task go:bench`

When adding tests:
- Prefer table-driven tests where it improves coverage and clarity.
- Add coverage for:
  - persistence/resume behavior (re-initialize and continue)
  - `WaitInitialize()` lifecycle correctness
  - `UpdateIndex()` correctness (especially around bulk dequeue)
  - boundary conditions: empty queue, queue close, max pages/rotation, queue size limits
  - concurrency: multiple producers (MPSC), single consumer invariants

## Pull Request Guidelines

When creating a pull request:

1. **Describe the contract**
   - What behavior changes (if any)? Does it affect on-disk format or compatibility?
2. **Tests first**
   - Add/adjust tests to cover the new behavior and edge cases.
3. **Performance impact**
   - If hot paths are touched, include benchmark results (`-bench` / `-benchmem`) and briefly interpret deltas.
4. **Race safety**
   - Run `task go:test` (or explain why it cannot be run in the environment).
5. **Docs & examples**
   - Update `README.md` and/or `examples/` when public usage, options, or lifecycle guidance changes.
6. **No config foot-guns**
   - README warns that options should not be changed once running; keep or strengthen safeguards where possible.

### Pre-commit Checklist

Before submitting your PR, ensure you have:
- [ ] `task go:test` passes
- [ ] Benchmarks executed if hot paths changed (`task go:bench`)
- [ ] `gofmt` applied to all changed `.go` files (`gofmt -w .`)
- [ ] Public API/doc changes reflected in `README.md` and/or `examples/`
- [ ] Any on-disk or behavior compatibility concerns explicitly documented in the PR description

### Code Review Checklist

For core queue changes (enqueue/dequeue/bulk, rotation, index):
- [ ] Concurrency assumptions are explicit (SPSC vs MPSC) and validated
- [ ] No new allocations in tight loops without justification
- [ ] Correctness preserved for `WaitInitialize()` + consumer lifecycle
- [ ] Index updates are correct for both single and bulk dequeue flows
- [ ] Queue close behavior is correct (`IsErrQueueClose` flow, `CloseIndex`)

For options/config changes:
- [ ] Defaults remain sane and documented
- [ ] Options remain safe w.r.t. persistence (no silent incompatibility)
- [ ] Changes include tests covering misconfiguration and boundaries

For IO/fsync-related changes:
- [ ] Behavior is deterministic and tested (where possible)
- [ ] Performance impact is measured with benchmarks
- [ ] Failure modes (partial writes, reopen/recover) are considered
