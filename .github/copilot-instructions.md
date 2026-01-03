
# .github/copilot-instructions.md

These are repository-wide custom instructions for GitHub Copilot (Chat / code review / coding agent).  
Keep instructions short and follow them even when the prompt is vague.

## Project context (FFQ)
- This repository is **FFQ (File-Based FIFO Queue)**: a high-performance, file-backed FIFO queue library for Go.
- FFQ is **stdlib-only**. Avoid adding external dependencies.
- Performance is dominated by **serialization and I/O**; changes must be allocation-aware.
- Default serialization is `json.Marshal` / `json.Unmarshal`, but encoder/decoder can be customized.

## Key semantics you MUST preserve
- Consumers typically:
  1) start the dequeue loop,
  2) call `WaitInitialize()` before producers start,
  3) call `UpdateIndex(...)` after successful dequeue,
  4) on queue close, call `CloseIndex()` and exit the consumer goroutine.
- Because the queue is file-backed, restart/resume behavior depends on correct index handling.

## How to propose and implement changes
- Prefer **minimal diffs** and preserve existing public APIs and naming.
- If you change behavior, update **tests** and **README/examples** to match.
- For changes touching concurrency (goroutines/locks/atomics/channels/pools), always:
  - add/adjust tests,
  - run with race detector,
  - call out safety assumptions (SPSC vs multi-producer).

## Build / Test commands (use these in suggestions and PR guidance)
- `task go:test`
- `task go:bench` (recommended for hot-path changes)

## Code style
- Use idiomatic Go.
- Run gofmt.
- Keep library code deterministic; avoid panics unless the codebase already treats the path as unrecoverable.
- Avoid extra abstractions in hot paths; be explicit about trade-offs.

## Reviews (when asked to review a PR)
- Focus on: correctness, shutdown/close behavior, race risks, allocation hot spots, file/index semantics, and benchmark impact.
- If a change introduces dependency, breaking API, or alters `WaitInitialize`/index semantics, flag it prominently.
