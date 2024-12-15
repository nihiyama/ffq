<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-dark.drawio.svg">
        <source media="(prefers-color-scheme: light)" srcset="./assets/logo-light.drawio.svg">
        <img alt="FFQ logo" src="./assets/logo-dark.drawio.svg" width="250">
    </picture>
</div>

FFQ (File-Based FIFO Queue) is a file-based system for managing FIFO queues. FFQ is developed using only the standard modules of Golang, without relying on any external modules or libraries. As a result, there is no need to manage dependencies, making it simple and lightweight. FFQ provides a simple and high-level API, making it easy to perform queue operations. With support for generics, you can enqueue data of any type into the queue. FFQ is designed for high performance, with speed primarily dependent on serialization and I/O during queue operations. By default, FFQ uses json.Marshal and json.Unmarshal for serialization, but you can customize it by providing functions with compatible interfaces for serialization and deserialization.  
Since FFQ stores the queue on files, even if the system goes down, you can resume reading from the queue without losing the progress of dequeued items. This implementation ensures reliability and data persistence, allowing seamless continuation from where you left off.

[![](https://img.shields.io/github/actions/workflow/status/nihiyama/ffq/test.yaml?branch=main&longCache=true&label=Test&logo=github%20actions&logoColor=fff)](https://github.com/nihiyama/ffq/actions?query=workflow%3ATest)
[![GoDoc](https://img.shields.io/badge/doc-reference-00ADD8.svg?logo=go)](https://pkg.go.dev/github.com/nihiyama/ffq)
[![Go Report Card](https://goreportcard.com/badge/github.com/nihiyama/ffq)](https://goreportcard.com/report/github.com/nihiyama/ffq)
[![Coverage Status](https://coveralls.io/repos/github/nihiyama/ffq/badge.svg?branch=main)](https://coveralls.io/github/nihiyama/ffq?branch=main)

## Usage

We provide both SimpleQueue and GroupQueue. A GroupQueue is a collection of SimpleQueue instances, where each SimpleQueue is managed under a unique ID, allowing efficient management of multiple queues. For more information about each type of queue, please refer to the [Architecture section](#architecture). Choose the queue type that best suits your use case.

For detailed usage, you can refer to the [code examples](./examples/README.md) or check the documentation on [GoDoc](https://pkg.go.dev/github.com/nihiyama/ffq).


### Simple Queue

```go
func main() {
    // Data is a struct with any field.
    q, err := NewQueue[Data]("example")
    if err != nil {
        // catch serious error
        panic(err)
    }

    var wg sync.WaitGroup

    // startup dequeue goroutine
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        for {
            // continue to dequeue repeatedly
            ms, err := q.BulkDequeue(size, lazy)
            if err != nil {
                if ffq.IsErrQueueClose(err) {
                    // if qeueu is closed, close index and finish goroutine
                    q.CloseIndex()
                    return
                }
                // catch serious error
                panic(err)
            }
            if len(ms) > 0 {
                // update index
                q.UpdateIndex(ms[len(ms)-1])
            }
        }
    }(&wg)

    // Initialization is performed after starting the dequeue goroutine 
    // and before starting the enqueue goroutine.
    // This ensures that data that has not yet been dequeued can be safely dequeued.
    q.WaitInitialize()

    // startup enqueue goroutine
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        q.BulkEnqueue(data)

        // finally, queueu is closed
        q.CloseQueue()
    }(&wg)

    wg.Wait()
}
```

### Group Queue

```go
func main() {
    // Data is a struct with any field.
    gq, err := NewGroupQueue[Data]("example")
    if err != nil {
        // catch serious error
        panic(err)
    }

    var wg sync.WaitGroup

    // startup dequeue goroutine
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        for {
            // continue to dequeue repeatedly
            // In groupqueue []*ffq.Message type is returned.
            msc, err := gq.BulkDequeue(size, lazy)
            if err != nil {
                if ffq.IsErrQueueClose(err) {
                    // if qeueu is closed, close index and finish goroutine
                    q.CloseIndex()
                    return
                }
                // catch serious error
                panic(err)
            }
            for ms := range msc {
                if len(ms) > 0 {
                    // update index
                    q.UpdateIndex(ms[len(ms)-1])
                }
            }
        }
    }(&wg)

    // Initialization is performed after starting the dequeue goroutine 
    // and before starting the enqueue goroutine.
    // This ensures that data that has not yet been dequeued can be safely dequeued.
    q.WaitInitialize()

    // startup enqueue goroutine 1.
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        gq.BulkEnqueue("q1", data)

        // finally, queueu is closed
        q.CloseQueue()
    }(&wg)

    // startup enqueue goroutine 2.
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        gq.BulkEnqueue("q2", data)

        // finally, queueu is closed
        q.CloseQueue()
    }(&wg)

    wg.Wait()
}
```

### Options

When creating a Queue instance, you can set options.
Options include the following.

| function name | type | default | detail |
| --- | --- | --- | --- |
| WithFileDir | string | `/tmp/ffq` | WithFileDir sets the directory where the queue files are stored. |
| WithQueueSize | int | `100` | WithQueueSize sets the maximum number of items that can be held in the queue. |
| WithMaxPages | int | `2` | WithMaxPages sets the number of files used in a single rotation cycle. |
| WithEncoder | func(v any) ([]byte, error) | `json.Marshal` | WithEncoder sets a custom encoder function |
| WithDecoder | func(data []byte, v any) error | `json.Unmarshal` | WithDecoder sets a custom decoder function. |

When you create an instance using the NewQueue or NewGroupQueue function you can give options.

```go
func main() {
    q, err := NewQueue[Data](
        "example",
        ffq.WithFileDir("/tmp"),
        ffq.WithQueueSize(100),
        ...
    )
}
```

> [!NOTE]  
> Once the options are set and running, do not change them. Doing so may cause data inconsistencies. If you wish to change an option, make sure that there are no outstanding queues, and while ffq is not running, delete the entire queue management directory before changing the option.

## Architecture

comming soon...

## Benchmark

A dataset with 12 keys was prepared, where each key has corresponding string, int, slice, and map values. This dataset was used to conduct benchmark tests at four different scales: 10, 100, 1000, and 10000 data points.

```
goos: linux
goarch: arm64
pkg: github.com/nihiyama/ffq
BenchmarkSimpleQueueEnqueueDequeue/Size10-8                 5815            188403 ns/op           27523 B/op        304 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size100-8                1081           1106183 ns/op          272627 B/op       3006 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size1000-8                115          10173246 ns/op         2805859 B/op      30026 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size10000-8                12          97968462 ns/op        27669352 B/op     300093 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size10-8            14205             83792 ns/op           28102 B/op        280 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size100-8            2817            422668 ns/op          354145 B/op       2716 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size1000-8                    327           3592632 ns/op         4179689 B/op      27059 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size10000-8                    31          35433528 ns/op        48759354 B/op     270433 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size10-8                   1711            762666 ns/op          118747 B/op       1083 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size100-8                   445           2505991 ns/op          897039 B/op       9311 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size1000-8                   70          17469755 ns/op         8547570 B/op      90740 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size10000-8                   8         138459084 ns/op        83293945 B/op     901250 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size10-8                100          12532097 ns/op          115502 B/op        979 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size100-8                81          13359309 ns/op         1425902 B/op       8304 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size1000-8               75          15646925 ns/op        13932777 B/op      81244 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size10000-8              22          51524816 ns/op        107287254 B/op    810956 allocs/op
PASS
ok      github.com/nihiyama/ffq 59.291s
```

