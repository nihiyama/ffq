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
BenchmarkSimpleQueueEnqueueDequeue/Size10-8                 1434            927992 ns/op          688181 B/op        340 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size100-8                 112          11013050 ns/op         6882086 B/op       3527 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size1000-8                  9         117325726 ns/op        69366014 B/op      51191 allocs/op
BenchmarkSimpleQueueEnqueueDequeue/Size10000-8                 1        1354899652 ns/op        748457352 B/op   2004664 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size10-8            10000            115531 ns/op           94017 B/op        284 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size100-8            1390            858927 ns/op          708340 B/op       2739 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size1000-8                    144           7986272 ns/op         6819537 B/op      28264 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue/Size10000-8                    13          79890743 ns/op        73710352 B/op     399966 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size10-8                    384           3262766 ns/op         2305752 B/op       1235 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size100-8                    46          23923244 ns/op        20967569 B/op      11112 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size1000-8                    6         184673588 ns/op        207700974 B/op    129792 allocs/op
BenchmarkGroupQueueEnqueueDequeue_3Group/Size10000-8                   1        1635014396 ns/op        2127005496 B/op  2698160 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size10-8                100          12549785 ns/op          518852 B/op       1001 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size100-8                84          13496887 ns/op         2428324 B/op       8484 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size1000-8               52          22218009 ns/op        20727282 B/op      84615 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue_3Group/Size10000-8               9         123806228 ns/op        215920584 B/op   1000641 allocs/op
PASS
ok      github.com/nihiyama/ffq 46.462s
```

## Note

if change maxPages, maxPages, dataFixedLength
remove `fileDir/name` directory and start
