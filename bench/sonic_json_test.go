package bench

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/nihiyama/ffq"
)

func BenchmarkSonicJSONSimpleQueueEnqueueDequeue(b *testing.B) {
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				q, err := ffq.NewQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				if err != nil {
					panic(err)
				}

				q.WaitInitialize()
				var wg sync.WaitGroup
				wg.Add(2)

				b.StartTimer()
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for _, d := range data {
						q.Enqueue(d)
					}
					q.CloseQueue()
				}(&wg)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						m, err := q.Dequeue()
						if ffq.IsErrQueueClose(err) {
							q.CloseIndex()
							return
						} else {
							q.UpdateIndex(m)
						}
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}

func BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue(b *testing.B) {
	var size uint64 = 100
	lazy := 10 * time.Millisecond
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				q, _ := ffq.NewQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				q.WaitInitialize()
				var wg sync.WaitGroup
				wg.Add(2)

				b.StartTimer()
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					q.BulkEnqueue(data)
					q.CloseQueue()
				}(&wg)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						ms, err := q.BulkDequeue(size, lazy)
						if ffq.IsErrQueueClose(err) {
							q.CloseIndex()
							return
						} else {
							if len(ms) > 0 {
								q.UpdateIndex(ms[len(ms)-1])
							}
						}
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}

func BenchmarkSonicJSONSimpleQueueEnqueueDequeue_5MP(b *testing.B) {
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				q, err := ffq.NewQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithQueueType(ffq.MPSC),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				if err != nil {
					panic(err)
				}

				q.WaitInitialize()
				var wg sync.WaitGroup
				var wgEnqueue sync.WaitGroup
				wg.Add(2)

				b.StartTimer()
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for i := 0; i < 5; i++ {
						wgEnqueue.Add(1)
						go func(wg *sync.WaitGroup) {
							defer wg.Done()
							for _, d := range data {
								q.Enqueue(d)
							}
						}(&wgEnqueue)
					}
					wgEnqueue.Wait()
					q.CloseQueue()
				}(&wg)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						m, err := q.Dequeue()
						if ffq.IsErrQueueClose(err) {
							q.CloseIndex()
							return
						} else {
							q.UpdateIndex(m)
						}
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}

func BenchmarkSonicJSONSimpleQueueBulkEnqueueDequeue_5MP(b *testing.B) {
	var size uint64 = 100
	lazy := 10 * time.Millisecond
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				q, _ := ffq.NewQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithQueueType(ffq.MPSC),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				q.WaitInitialize()
				var wg sync.WaitGroup
				var wgEnqueue sync.WaitGroup
				wg.Add(2)

				b.StartTimer()

				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for i := 0; i < 5; i++ {
						wgEnqueue.Add(1)
						go func(wg *sync.WaitGroup) {
							defer wg.Done()
							q.BulkEnqueue(data)
						}(&wgEnqueue)
					}
					wgEnqueue.Wait()
					q.CloseQueue()
				}(&wg)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						ms, err := q.BulkDequeue(size, lazy)
						if ffq.IsErrQueueClose(err) {
							q.CloseIndex()
							return
						} else {
							if len(ms) > 0 {
								q.UpdateIndex(ms[len(ms)-1])
							}
						}
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}

func BenchmarkSonicJSONGroupQueueEnqueueDequeue_5Group(b *testing.B) {
	testQueues := []string{"queue1", "queue2", "queue3", "queue4", "queue5"}
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				gq, _ := ffq.NewGroupQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				gq.WaitInitialize()
				var wg sync.WaitGroup
				var wgEnqueue sync.WaitGroup
				wg.Add(2)

				b.StartTimer()
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for _, tq := range testQueues {
						wgEnqueue.Add(1)
						go func(wg *sync.WaitGroup, name string) {
							defer wg.Done()
							for _, d := range data {
								gq.Enqueue(name, d)
							}
						}(&wgEnqueue, tq)
					}
					wgEnqueue.Wait()
					gq.CloseQueue()
				}(&wg)

				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						m, err := gq.Dequeue()
						if err != nil {
							if ffq.IsErrQueueClose(err) {
								gq.CloseIndex()
								return
							}
						}
						gq.UpdateIndex(m)
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}

func BenchmarkSonicJSONGroupQueueBulkEnqueueDequeue_5Group(b *testing.B) {
	testQueues := []string{"queue1", "queue2", "queue3", "queue4", "queue5"}
	size := 100
	lazy := 10 * time.Millisecond
	for _, tt := range tests {
		b.Run(fmt.Sprintf("Size%d", tt), func(b *testing.B) {
			dir, _ := os.MkdirTemp("", "ffqbenchtest")
			data := createData(tt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				gq, _ := ffq.NewGroupQueue[BenchmarkData](
					"benchmark",
					ffq.WithFileDir(dir),
					ffq.WithQueueSize(1024),
					ffq.WithMaxPage(3),
					ffq.WithEncoder(sonic.Marshal),
					ffq.WithDecoder(sonic.Unmarshal),
				)
				gq.WaitInitialize()
				var wg sync.WaitGroup
				var wgEnqueue sync.WaitGroup
				wg.Add(2)

				b.StartTimer()
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for _, tq := range testQueues {
						wgEnqueue.Add(1)
						go func(wg *sync.WaitGroup, name string) {
							defer wg.Done()
							gq.BulkEnqueue(name, data)
						}(&wgEnqueue, tq)
					}
					wgEnqueue.Wait()
					gq.CloseQueue()
				}(&wg)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					for {
						ms, err := gq.BulkDequeue(uint64(size), lazy)
						if err != nil {
							if ffq.IsErrQueueClose(err) {
								gq.CloseIndex()
								return
							}
						}
						for _, m := range ms {
							gq.UpdateIndex(m)
						}
					}
				}(&wg)
				wg.Wait()
				b.StopTimer()

				os.RemoveAll(dir)
			}
		})
	}
}
