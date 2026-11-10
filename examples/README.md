# FFQ examples

Runnable examples are maintained in [usage_example_test.go](../usage_example_test.go). They cover SimpleQueue and GroupQueue, single and bulk operations, and the startup/shutdown lifecycle with initialization error handling.

Run them from the repository root:

```sh
go test -run '^Example' .
```

These examples are also included in `task go:test`. See the [usage documentation](../README.md#usage) and [compatibility notes](../docs/compatibility.md).
