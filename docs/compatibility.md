# Storage Compatibility and Recovery Limits

English | [日本語](compatibility_ja.md)

These changes preserve existing exported API names, parameter types, return types, and the on-disk format.

- Data remains in `queue.N`, with one JSON array per line. The configured page count and directory layout are unchanged.
- `index` remains an 8-byte little-endian circular position. A zero-byte index is valid before the first successful progress update.
- No generation numbers, checksums, headers, auxiliary files, format identifiers, migration steps, or permission changes are introduced.
- `Message.Index()` still returns the circular position. The private `sequence` and `owner` fields exist only in memory; the queue name is obtained from `owner`.

Initialization validates existing files before delivering recovered messages. It does not delete or truncate retained pages to guess their history. Page reuse is deferred until enqueue and waits until a successful index write acknowledges the page's previous contents. An index write failure or short write does not advance in-memory progress or permit page reuse.

## Startup, Processing, and Shutdown

Start the consumer before calling `WaitInitializeWithError()`, and start producers only if it returns nil. After initialization completes, the new API returns recorded initialization errors or fatal failures, or `ErrQueueClose` if the queue is closed. Nil does not guarantee future operations will succeed or that all recovered messages have finished business processing. Continue checking enqueue, dequeue, and progress-update errors. The existing `WaitInitialize()` remains available, but it only waits for completion and cannot indicate success. Because the new API also checks failures recorded at the time of the call, repeated calls can return different results after a later failure or close.

Call `UpdateIndex()` after processing a message successfully. For a bulk result, update the index of the last completed message in each child queue, or update each completed message individually. Keep the consumer running while waiting for producers to finish. Once all producers have finished, the coordinator closes the queue once; the consumer closes the index after draining the remaining messages.

`Length()` counts only messages in the channel. Messages already held by the consumer still protect their pages until progress is saved. Enqueue can therefore wait at a page boundary even when the channel has space. The `FuncAfter*` helpers preserve their existing callback and acknowledgment behavior. If a failed item must remain unacknowledged, process it explicitly and call `UpdateIndex()` only after success.

## Initialization Errors and Failure Reporting

File-operation, JSON-parsing, and decoder failures during background initialization are retained as initialization errors. A decoder panic during initialization is converted to a generic initialization error without exposing the original panic value, stack, or payload. The library does not automatically log, alert, or terminate the process. Encoder or processing-callback panics outside initialization are not uniformly converted to errors.

Background initialization can fail after `NewQueue` has returned successfully. Start the consumer first and check the error from `WaitInitializeWithError()`. Silently returning from the consumer on failure can leave the process alive without a working consumer. Notify the coordinator and update monitoring, admission control, and shutdown handling. Initialization completion does not mean that recovered messages have finished business processing or that their progress has been saved.

If business processing fails during recovery, notify the coordinator before calling `CloseIndex()`, which waits for initialization to complete. The coordinator must monitor both initialization completion and consumer failures. If producers have not started, it can call `CloseQueue()` to release internal replay waits, then wait for shutdown to finish. After normal operation has begun, shutdown must be coordinated with producers; do not run enqueue and close concurrently without coordination.

`CloseQueue()` does not guarantee cancellation of unresponsive NFS reads, stat calls, closes, or other I/O. Neither `CloseQueue()` nor `CloseIndex()` is an initialization-status API. A nil result from a repeated close does not establish that the first close failure has been resolved.

## Bulk Limits and Error Handling Fixes

Both queue types require a positive limit for `BulkDequeue` and `FuncAfterBulkDequeue`. A zero limit returns an error wrapping `ErrQueueOption` before waiting, without consuming messages, invoking the callback, or updating progress. Argument validation happens first, so zero also returns this error for a closed queue or a GroupQueue with no children. A one-item limit returns after the first item without waiting for `lazy`.

This fixes the zero/one-item boundary cases and defines the previously unspecified zero-limit behavior. If an application used a zero limit on GroupQueue as an empty operation, skip that call instead. Existing public function names, parameters, and return types are unchanged.

`GroupQueue.FuncAfterBulkDequeue` now returns callback and index-update errors to the caller. It retains the previous behavior of attempting to acknowledge progress even when the callback returns an error. Check the returned error; if processing failures must leave messages unacknowledged, use explicit processing followed by `UpdateIndex()`. Errors for a missing child queue are now recognizable with `IsErrQueueNotFound`.

After a normal GroupQueue close, single and bulk enqueue return errors recognizable with `IsErrQueueClose`, for both existing and missing child names, without creating a child directory. Replace comparisons against strings such as `already closed` with the error helpers. A previously recorded fatal failure can take precedence over the close error.

## Recovery from a Cycle-End Index

When the index points to the end of a complete cycle and the next cycle ends partway through the physical last page, recovery normalizes positions into that cycle's coordinate range. This includes the 2048-item × 3-page case with 6144 completed items and 4097 pending items, while preserving the retained page bytes.

This correction applies only when all pages corresponding to the preceding cycle are retained and nonempty. Inconsistent states with missing or empty pages are still rejected while preserving the files. Timestamp ambiguity, including states where all pages are full, remains subject to the limits below.

## Changing T after Processing Completes

After all items of the old T have been processed and their progress saved successfully with `UpdateIndex()`, close the queue and index before reopening the same directory with a different T. Keep storage settings, including page count and queue size, unchanged.

Completed records of the old T and subsequent records of the new T may occupy different lines in the same `queue.N`. Item counting and trailing-data validation use `json.RawMessage` independently of T. Completed records are skipped without decoding them into the current T; only pending records are decoded into the current T. A different JSON structure beginning on the next line is therefore not rejected by the additional trailing-data check.

Changing T while old-T messages remain pending is outside this usage contract. Per-line JSON-array validation and the timestamp-based recovery limits still apply.

## Internal Sequence Limit

`q.sequence` is a process-local `uint64` position that advances for each item. `m.sequence` retains the value assigned when the Message was created; it does not increase afterward. Before writing, the implementation checks whether adding the item count would exceed the limit. A write that would overflow puts the instance into a failed state rather than wrapping to zero and continuing. The final valid Message has sequence `MaxUint64-1`; acknowledging it advances `ackNext` to `MaxUint64`.

The sequence is not persisted. After successful initialization, sequence, progress, and page-end positions are reconstructed from retained data. The counter does not accumulate every historical enqueue across restarts. Regression tests cover the upper boundary, pending-page protection, MPSC, and reopen behavior. States that the existing format cannot disambiguate remain subject to the limits below.

## States the Existing Format Cannot Disambiguate

Recovery uses page contents, item counts, the circular index, and file modification times. The existing files contain no persisted generation number.

For example, a fully completed cycle and a later cycle containing identical pending values can have the same data bytes and index. If their modification times also coincide, the histories cannot be distinguished. Rapid writes followed by shutdown can produce matching timestamps during normal operation, even when no items remain pending. An identifiable partial page can resolve some equal-timestamp cases, but this is not guaranteed for every history involving partial pages.

If the available information cannot uniquely determine the correct pending range, initialization returns an error and preserves the files. It does not silently return an empty queue or choose a history that could lose or replay messages. Keeping the storage format unchanged therefore means more initialization states can be rejected than with the previous implementation.

Recovery does not guarantee correct handling of arbitrary timestamp reversal or modification, every corruption that turns an index into another valid in-range value, configuration changes, or every possible existing state. Do not automatically delete files or change settings to work around an initialization error.

## Directory Ownership and Durability

Each queue directory must have one owner, and its settings must remain unchanged across restart. Concurrent enqueue/close and sharing a directory between processes are outside the supported shutdown and ownership protocols. No cross-process lock or transaction guaranteeing exactly-once external side effects has been added.

`SetFSync()` retains the existing `O_SYNC` flags for data and index files. Process-interruption tests do not guarantee durability under power loss, OS crashes, filesystem or device failures, or atomic index writes. A bulk enqueue error can occur after some data has already been accepted; unconditional retries do not necessarily prevent duplicates. After a fatal I/O failure, do not assume the instance remains usable: reopen it.

The reported production configuration uses NFS without `SetFSync()`. Local-storage tests in the development container have not validated NFS timestamp discrimination, performance, connection failures, or durability. See the [v0.3.1 upgrade checklist](v0.3.1-upgrade.md) for production-like NFS validation and rollback procedures.

Storage-format regression tests remain in `recovery_test.go`, `write_failure_linux_test.go`, and related files, using the released queue fixtures in `testdata/released-queues.json`. See [usage_example_test.go](../usage_example_test.go) for runnable examples.
