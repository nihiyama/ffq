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
BenchmarkSimpleQueueEnqueueDequeue10                        1651            754365 ns/op          686282 B/op        335 allocs/op
BenchmarkSimpleQueueEnqueueDequeue100                        100          10570790 ns/op         6867607 B/op       3500 allocs/op
BenchmarkSimpleQueueEnqueueDequeue1000                         9         119574377 ns/op        69265865 B/op      50915 allocs/op
BenchmarkSimpleQueueEnqueueDequeue10000                        1        1726579573 ns/op        748276128 B/op   2004173 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue10                   12860             89705 ns/op           92442 B/op        283 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue100                   1122            983212 ns/op          698561 B/op       2737 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue1000                   138           8275012 ns/op         6805284 B/op      28304 allocs/op
BenchmarkSimpleQueueBulkEnqueueDequeue10000                   12          89143218 ns/op        74057737 B/op     410624 allocs/op
BenchmarkGroupQueueEnqueueDequeue10_3Group                   336           3214467 ns/op         2287377 B/op       1087 allocs/op
BenchmarkGroupQueueEnqueueDequeue100_3Group                   38          29182965 ns/op        20823214 B/op      10039 allocs/op
BenchmarkGroupQueueEnqueueDequeue1000_3Group                   4         310158024 ns/op        207489608 B/op    135722 allocs/op
BenchmarkGroupQueueEnqueueDequeue10000_3Group                  1        4208820685 ns/op        2125512056 B/op  2684781 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue10_3Group               777           1501167 ns/op          502994 B/op        947 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue100_3Group              327           3510217 ns/op         2318779 B/op       8307 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue1000_3Group              49          23034984 ns/op        20599642 B/op      84641 allocs/op
BenchmarkGroupQueueBulkEnqueueDequeue10000_3Group              1        1021066285 ns/op        712936816 B/op   9052384 allocs/op
```

## Note

if change pageSize, pageSize, dataFixedLength
remove `fileDir/name` directory and start
