package ffq

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Synchronous write settings.

func TestSetFsync(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "no set",
			want: os.O_RDWR | os.O_CREATE,
		},
		{
			name: "set",
			want: os.O_RDWR | os.O_CREATE | os.O_SYNC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeFOpenFlag, beforeFCreateFlag := fOpenFlag, fCreateFlag
			defer func() {
				fOpenFlag, fCreateFlag = beforeFOpenFlag, beforeFCreateFlag
			}()

			if tt.name == "set" {
				SetFSync()
			}
			if fOpenFlag != tt.want {
				t.Errorf("flag got = %d, want = %d", fOpenFlag, tt.want)
			}
		})
	}
}

// Synchronous steady-state benchmarks.

type syncBenchmarkItem struct {
	Value   int
	Payload string
}

func BenchmarkQueueSteadyStateSync(b *testing.B) {
	for _, producers := range []int{1, 4} {
		b.Run(fmt.Sprintf("Producers%d/Batch50", producers), func(b *testing.B) {
			beforeOpen, beforeCreate := fOpenFlag, fCreateFlag
			defer func() { fOpenFlag, fCreateFlag = beforeOpen, beforeCreate }()
			SetFSync()
			kind := SPSC
			if producers > 1 {
				kind = MPSC
			}
			q, err := NewQueue[syncBenchmarkItem]("bench", WithFileDir(b.TempDir()), WithQueueSize(2048), WithMaxPage(3), WithQueueType(kind))
			if err != nil {
				b.Fatal(err)
			}
			items := make([]*syncBenchmarkItem, 50)
			for i := range items {
				items[i] = &syncBenchmarkItem{Value: i, Payload: strings.Repeat("x", 1024)}
			}
			consumer := make(chan error, 1)
			go func() {
				for {
					messages, err := q.BulkDequeue(50, 10*time.Millisecond)
					if IsErrQueueClose(err) {
						consumer <- q.CloseIndex()
						return
					}
					if err != nil {
						consumer <- err
						return
					}
					if len(messages) > 0 {
						if err := q.UpdateIndex(messages[len(messages)-1]); err != nil {
							consumer <- err
							return
						}
					}
				}
			}()
			q.WaitInitialize()
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for producer := 0; producer < producers; producer++ {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					for i := producer; i < b.N; i += producers {
						if err := q.BulkEnqueue(items); err != nil {
							b.Error(err)
							return
						}
					}
				}(producer)
			}
			wg.Wait()
			if err := q.CloseQueue(); err != nil {
				b.Error(err)
			}
			if err := <-consumer; err != nil {
				b.Error(err)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)*50/b.Elapsed().Seconds(), "items/s")
		})
	}
}
