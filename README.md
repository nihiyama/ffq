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
pkg: github.com/nihiyama/ffq/bench
BenchmarkGoJSONSimpleQueueEnqueueDequeue/Size10-8                  10000            181210 ns/op           28741 B/op         44 allocs/op
BenchmarkGoJSONSimpleQueueEnqueueDequeue/Size100-8                  1712            692693 ns/op          286619 B/op        407 allocs/op
BenchmarkGoJSONSimpleQueueEnqueueDequeue/Size1000-8                  300           3857455 ns/op         2797539 B/op       4018 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue/Size10-8              24903             49034 ns/op           29538 B/op         21 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue/Size100-8              3540            296702 ns/op          455527 B/op        118 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue/Size1000-8              454           2645481 ns/op         7374591 B/op       1068 allocs/op
BenchmarkGoJSONSimpleQueueEnqueueDequeue_5MP/Size10-8               3736            530937 ns/op          144174 B/op        216 allocs/op
BenchmarkGoJSONSimpleQueueEnqueueDequeue_5MP/Size100-8               421           2673111 ns/op         1442705 B/op       2036 allocs/op
BenchmarkGoJSONSimpleQueueEnqueueDequeue_5MP/Size1000-8               45          23225262 ns/op        14019820 B/op      20127 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue_5MP/Size10-8           5536            204298 ns/op          135926 B/op         81 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue_5MP/Size100-8                   847           1387673 ns/op         1980260 B/op        573 allocs/op
BenchmarkGoJSONSimpleQueueBulkEnqueueDequeue_5MP/Size1000-8                   78          15155019 ns/op        35761256 B/op       5355 allocs/op
BenchmarkGoJSONGroupQueueEnqueueDequeue_5Group/Size10-8                     1976            708317 ns/op          207140 B/op        464 allocs/op
BenchmarkGoJSONGroupQueueEnqueueDequeue_5Group/Size100-8                     627           1750355 ns/op         1600943 B/op       2322 allocs/op
BenchmarkGoJSONGroupQueueEnqueueDequeue_5Group/Size1000-8                     98          11004677 ns/op        14102366 B/op      20367 allocs/op
BenchmarkGoJSONGroupQueueBulkEnqueueDequeue_5Group/Size10-8                 3824            300221 ns/op          212297 B/op        326 allocs/op
BenchmarkGoJSONGroupQueueBulkEnqueueDequeue_5Group/Size100-8                 829           1377020 ns/op         3243249 B/op        858 allocs/op
BenchmarkGoJSONGroupQueueBulkEnqueueDequeue_5Group/Size1000-8                129           8533375 ns/op        35231912 B/op       5562 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue/Size10-8                            10000            231058 ns/op           40799 B/op        304 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue/Size100-8                            1443            861123 ns/op          405220 B/op       3007 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue/Size1000-8                            198           5934142 ns/op         3950594 B/op      30015 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue/Size10-8                        17487             62894 ns/op           40425 B/op        281 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue/Size100-8                        2642            431346 ns/op          488511 B/op       2715 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue/Size1000-8                        294           3993813 ns/op         5721652 B/op      27056 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue_5MP/Size10-8                         2808            612809 ns/op          204116 B/op       1516 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue_5MP/Size100-8                         279           4034488 ns/op         2044779 B/op      15033 allocs/op
BenchmarkJSONSimpleQueueEnqueueDequeue_5MP/Size1000-8                         32          32158689 ns/op        19854594 B/op     150107 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue_5MP/Size10-8                     4473            268459 ns/op          193614 B/op       1381 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue_5MP/Size100-8                     528           2246064 ns/op         2299095 B/op      13562 allocs/op
BenchmarkJSONSimpleQueueBulkEnqueueDequeue_5MP/Size1000-8                     55          21459433 ns/op        27839735 B/op     135297 allocs/op
BenchmarkJSONGroupQueueEnqueueDequeue_5Group/Size10-8                       1882            725399 ns/op          269376 B/op       1765 allocs/op
BenchmarkJSONGroupQueueEnqueueDequeue_5Group/Size100-8                       525           2311979 ns/op         2185272 B/op      15303 allocs/op
BenchmarkJSONGroupQueueEnqueueDequeue_5Group/Size1000-8                       86          14097208 ns/op        19934410 B/op     150354 allocs/op
BenchmarkJSONGroupQueueBulkEnqueueDequeue_5Group/Size10-8                   3285            317469 ns/op          273905 B/op       1626 allocs/op
BenchmarkJSONGroupQueueBulkEnqueueDequeue_5Group/Size100-8                   698           1626522 ns/op         3192447 B/op      13831 allocs/op
BenchmarkJSONGroupQueueBulkEnqueueDequeue_5Group/Size1000-8                  111          10180737 ns/op        31272311 B/op     135521 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue/Size10-8                       10000            182738 ns/op           33547 B/op         54 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue/Size100-8                       1268            793526 ns/op          317641 B/op        506 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue/Size1000-8                       249           4663699 ns/op         2883757 B/op       5012 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue/Size10-8                   21866             56680 ns/op           31869 B/op         22 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue/Size100-8                   3098            359794 ns/op          371931 B/op        114 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue/Size1000-8                   320           3644208 ns/op        11093796 B/op       1069 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue_5MP/Size10-8                    3592            570777 ns/op          165500 B/op        265 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue_5MP/Size100-8                    367           2962512 ns/op         1574216 B/op       2527 allocs/op
BenchmarkSonicJSONSimpleQueueEnqueueDequeue_5MP/Size1000-8                    39          26188324 ns/op        14512105 B/op      25088 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue_5MP/Size10-8                5238            222071 ns/op          143110 B/op         85 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue_5MP/Size100-8                685           1665518 ns/op         1713373 B/op        562 allocs/op
BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue_5MP/Size1000-8                54          21254885 ns/op        52341309 B/op       5385 allocs/op
BenchmarkSonicJSONGroupQueueEnqueueDequeue_5Group/Size10-8                  1948            710172 ns/op          256895 B/op        514 allocs/op
BenchmarkSonicJSONGroupQueueEnqueueDequeue_5Group/Size100-8                  620           1786605 ns/op         1770529 B/op       2783 allocs/op
BenchmarkSonicJSONGroupQueueEnqueueDequeue_5Group/Size1000-8                  96          11790819 ns/op        14796516 B/op      25320 allocs/op
BenchmarkSonicJSONGroupQueueBulkEnqueueDequeue_5Group/Size10-8              3591            313545 ns/op          251767 B/op        329 allocs/op
BenchmarkSonicJSONGroupQueueBulkEnqueueDequeue_5Group/Size100-8              858           1307271 ns/op         2398778 B/op        817 allocs/op
BenchmarkSonicJSONGroupQueueBulkEnqueueDequeue_5Group/Size1000-8             115          10232173 ns/op        56570411 B/op       5574 allocs/op
PASS
ok      github.com/nihiyama/ffq/bench   177.265s
```
