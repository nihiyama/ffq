<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-dark.drawio.svg">
        <source media="(prefers-color-scheme: light)" srcset="./assets/logo-light.drawio.svg">
        <img alt="FFQ logo" src="./assets/logo-dark.drawio.svg" width="250">
    </picture>
</div>

FFQ means File-Based FIFO Queue.

[![](https://img.shields.io/github/actions/workflow/status/nihiyama/ffq/test.yaml?branch=main&longCache=true&label=Test&logo=github%20actions&logoColor=fff)](https://github.com/nihiyama/ffq/actions?query=workflow%3ATest)
[![GoDoc](https://img.shields.io/badge/doc-reference-00ADD8.svg?logo=go)](https://pkg.go.dev/github.com/nihiyama/ffq)
[![Go Report Card](https://goreportcard.com/badge/github.com/nihiyama/ffq)](https://goreportcard.com/report/github.com/nihiyama/ffq)
[![Coverage Status](https://coveralls.io/repos/github/nihiyama/ffq/badge.svg?branch=main)](https://coveralls.io/github/nihiyama/ffq?branch=main)

## features

- [x] fifo queue with file
- [x] group queue
- [ ] examples
- [ ] test
- [ ] documentation

## Benchmark

A dataset with 12 keys was prepared, where each key has corresponding string, int, slice, and map values. This dataset was used to conduct benchmark tests at four different scales: 10, 100, 1000, and 10000 data points.

```
goos: linux
goarch: arm64
pkg: github.com/nihiyama/ffq
BenchmarkSimpleQueueEnqueueDequeue10                        1788            690305 ns/op          686282 B/op        335 allocs/op
BenchmarkSimpleQueueEnqueueDequeue100                        132          11015562 ns/op         6866321 B/op       3474 allocs/op
BenchmarkSimpleQueueEnqueueDequeue1000                         8         131244262 ns/op        69347153 B/op      53027 allocs/op
BenchmarkSimpleQueueEnqueueDequeue10000                        1        1607996331 ns/op        748274432 B/op   2004167 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue10                   12788             91125 ns/op           92442 B/op        283 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue100                   1125            957756 ns/op          698559 B/op       2737 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue1000                   135           8236663 ns/op         6807223 B/op      28329 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue10000                   12          92946453 ns/op        74057826 B/op     410625 allocs/op
BenchmarkGroupQueueEnqueueDequeue10_3Group                   319           3923860 ns/op         2289596 B/op       1140 allocs/op
BenchmarkGroupQueueEnqueueDequeue100_3Group                   32          36434364 ns/op        20834059 B/op      10643 allocs/op
BenchmarkGroupQueueEnqueueDequeue1000_3Group                   3         370326426 ns/op        208035389 B/op    152912 allocs/op
BenchmarkGroupQueueEnqueueDequeue10000_3Group                  1        4376431304 ns/op        2125482792 B/op  2684600 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue10_3Group                84          12465048 ns/op          504824 B/op        967 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue100_3Group               93          13596804 ns/op         2326885 B/op       8401 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue1000_3Group              37          31397757 ns/op        20666928 B/op      85736 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue10000_3Group              4         314572069 ns/op        228403846 B/op   1231677 allocs/op
PASS
ok      github.com/nihiyama/ffq 53.625s
```

## Note

if change pageSize, pageSize, dataFixedLength
remove `fileDir/name` directory and start
