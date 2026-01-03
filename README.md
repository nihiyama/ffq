<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-dark.drawio.svg">
        <source media="(prefers-color-scheme: light)" srcset="./assets/logo-light.drawio.svg">
        <img alt="FFQ logo" src="./assets/logo-dark.drawio.svg" width="250">
    </picture>
</div>

FFQ (File-Based FIFO Queue) is a file-based system for managing FIFO queues. FFQ is developed using only the standard modules of Golang, without relying on any external modules or libraries. As a result, there is no need to manage dependencies, making it simple and lightweight. FFQ provides a simple and high-level API, making it easy to perform queue operations. With support for generics, you can enqueue data of any type into the queue. FFQ is designed for high performance , with speed primarily dependent on serialization and I/O during queue operations (almost lock-free, completely lock-free using SPSC). By default, FFQ uses json.Marshal and json.Unmarshal for serialization, but you can customize it by providing functions with compatible interfaces for serialization and deserialization. The queue file format assumes JSON arrays per line, so custom codecs must remain JSON-compatible; non-JSON formats are not supported.  
Since FFQ stores the queue on files, even if the system goes down, you can resume reading from the queue without losing the progress of dequeued items. This implementation ensures reliability and data persistence, allowing seamless continuation from where you left off.

[![](https://img.shields.io/github/actions/workflow/status/nihiyama/ffq/test.yaml?branch=main&longCache=true&label=Test&logo=github%20actions&logoColor=fff)](https://github.com/nihiyama/ffq/actions?query=workflow%3ATest)
[![GoDoc](https://img.shields.io/badge/doc-reference-00ADD8.svg?logo=go)](https://pkg.go.dev/github.com/nihiyama/ffq)
[![Go Report Card](https://goreportcard.com/badge/github.com/nihiyama/ffq)](https://goreportcard.com/report/github.com/nihiyama/ffq)
[![Coverage Status](https://coveralls.io/repos/github/nihiyama/ffq/badge.svg?branch=main)](https://coveralls.io/github/nihiyama/ffq?branch=main)

## Usage

We provide both SimpleQueue and GroupQueue. A GroupQueue is a collection of SimpleQueue instances, where each SimpleQueue is managed under a unique ID, allowing efficient management of multiple queues. For more information about each type of queue, please refer to the [Architecture section](#architecture). Choose the queue type that best suits your use case.

For detailed usage, you can refer to the [code examples](./examples/README.md) or check the documentation on [GoDoc](https://pkg.go.dev/github.com/nihiyama/ffq).


### Simple Queue

For simplicity, the combination of Enqueue/Dequeue and BulkEnqueue/BulkDequeue is presented; however, feel free to　mix and match them to suit your use case.

#### Enqueue/Dequeue

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
            m, err := q.Dequeue()
            if err != nil {
                if ffq.IsErrQueueClose(err) {
                    // if qeueu is closed, close index and finish goroutine
                    q.CloseIndex()
                    return
                }
            }
            q.UpdateIndex(m)
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
        for _, d := range data {
            q.Enqueue(d)
        }

        // finally, queueu is closed
        q.CloseQueue()
    }(&wg)

    wg.Wait()
}
```

#### BulkEnqueue/BulkDequeue

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

For simplicity, the combination of Enqueue/Dequeue and BulkEnqueue/BulkDequeue, as with Simple Queue, is presented; however, feel free to mix and match them to suit your use case.

#### Enqueue/Dequeue

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
            m, err := gq.Dequeue()
            if err != nil {
                if ffq.IsErrQueueClose(err) {
                    // if qeueu is closed, close index and finish goroutine
                    gq.CloseIndex()
                    return
                }
            }
            gq.UpdateIndex(m)
        }
    }(&wg)

    // Initialization is performed after starting the dequeue goroutine 
    // and before starting the enqueue goroutine.
    // This ensures that data that has not yet been dequeued can be safely dequeued.
    gq.WaitInitialize()

    // startup enqueue goroutine 1.
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        for _, d := range data {
            gq.Enqueue("q1", d)
        }

        // finally, queueu is closed
        gq.CloseQueue()
    }(&wg)

    // startup enqueue goroutine 2.
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        for _, d := range data {
            gq.Enqueue("q2", d)
        }

        // finally, queueu is closed
        gq.CloseQueue()
    }(&wg)

    wg.Wait()
}
```

#### BulkEnqueue/BulkDequeue

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
            ms, err := gq.BulkDequeue(size, lazy)
            if err != nil {
                if ffq.IsErrQueueClose(err) {
                    // if qeueu is closed, close index and finish goroutine
                    gq.CloseIndex()
                    return
                }
            }
            if len(ms) > 0 {
                // update index
                for _, m := range ms {
                    m.UpdateIndex(m)
                }
            }
        }
    }(&wg)

    // Initialization is performed after starting the dequeue goroutine 
    // and before starting the enqueue goroutine.
    // This ensures that data that has not yet been dequeued can be safely dequeued.
    gq.WaitInitialize()

    // startup enqueue goroutine 1.
    wg.Add(1)
    go func(wg *sync.WaitGroup) {
        defer wg.Done()
        // data has []*Data type.
        data := makeData()

        // enqueue
        gq.BulkEnqueue("q1", data)

        // finally, queueu is closed
        gq.CloseQueue()
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
        gq.CloseQueue()
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
| WithQueueType | ffq.QueueType | `ffq.SPSC` | WithQueueType can use `ffq.SPSC` or `ffq.MPSC`. It is possible to switch between Single Producer Single Consumer and Multiple Producer Single Consumer. |
| WithGroupSize | int | `10` | WithGroupSize sets a GroupQueue size. It can only be used with GroupQueue. |

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

### Additional Settings

Before creating an instance of Queue, you can perform the following configurations. Execute them as needed.

| function name | type | default | detail |
| --- | --- | --- | --- |
| SetQueueBufferSize | int | 64kb | Buffer size used for writing to the Queue |
| SetFSync | - | no fsync | Executes SetFSync to wait for the OS to complete writes |

## Architecture

comming soon...

## Benchmark

A dataset with 12 keys was prepared, where each key has corresponding string, int, slice, and map values (more than 1kb). This dataset was used to conduct benchmark tests at four different scales: 10, 100 and 1000 data points.

```
goos: linux
goarch: arm64
pkg: github.com/nihiyama/ffq/ringbuffer
BenchmarkSimpleQueueEnqueueDequeue/Size10-8                10000            146467 ns/op           40225 B/op        304 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size100-8                1530            777684 ns/op          402447 B/op       3006 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size1000-8                210           5514527 ns/op         3960243 B/op      30015 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size10-8            22395             52355 ns/op           40308 B/op        281 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size100-8            3025            395634 ns/op          475160 B/op       2714 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size1000-8                    297           3941332 ns/op         5543107 B/op      27056 allocs/op
BenchmarkSimpleQueueEnqueueDequeue_5MP/Size10-8                     3229            558267 ns/op          201044 B/op       1515 allocs/op
BenchmarkSimpleQueueEnqueueDequeue_5MP/Size100-8                     297           3600200 ns/op         2020212 B/op      15029 allocs/op
BenchmarkSimpleQueueEnqueueDequeue_5MP/Size1000-8                     31          32455408 ns/op        19861525 B/op     150117 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue_5MP/Size10-8                 4480            257337 ns/op          192031 B/op       1380 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue_5MP/Size100-8                 552           2095460 ns/op         2193344 B/op      13561 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue_5MP/Size1000-8                 52          21862708 ns/op        29965125 B/op     135308 allocs/op
BenchmarkGroupQueueEnqueueDequeue_5Group/Size10-8                   1465            751427 ns/op          266635 B/op       1762 allocs/op
BenchmarkGroupQueueEnqueueDequeue_5Group/Size100-8                   530           2222183 ns/op         2182742 B/op      15301 allocs/op
BenchmarkGroupQueueEnqueueDequeue_5Group/Size1000-8                   94          12747984 ns/op        19951426 B/op     150356 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_5Group/Size10-8               2799            421194 ns/op          263407 B/op       1624 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_5Group/Size100-8               753           1662122 ns/op         3206951 B/op      13830 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_5Group/Size1000-8              111          10224487 ns/op        29013525 B/op     135512 allocs/op
PASS
ok      github.com/nihiyama/ffq/ringbuffer      51.898s
```
