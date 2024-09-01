package ffq_test

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nihiyama/ffq"
)

func init() {
	runtime.GOMAXPROCS(1)
}

type BenchmarkData struct {
	Val1  string
	Val2  int
	Val3  []string
	Val4  map[string]string
	Val5  string
	Val6  int
	Val7  []string
	Val8  map[string]string
	Val9  string
	Val10 int
	Val11 []string
	Val12 map[string]string
}

func createData(n int) []*BenchmarkData {
	data := make([]*BenchmarkData, 0, n)
	for i := 0; i < n; i++ {
		val3 := make([]string, 0, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val4 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		val7 := make([]string, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val8 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		val11 := make([]string, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val12 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		d := BenchmarkData{
			Val1:  fmt.Sprintf("string val1, %d", i),
			Val2:  i * 2,
			Val3:  val3,
			Val4:  val4,
			Val5:  fmt.Sprintf("string val5, %d", i),
			Val6:  i * 6,
			Val7:  val7,
			Val8:  val8,
			Val9:  fmt.Sprintf("string val9, %d", i),
			Val10: i * 10,
			Val11: val11,
			Val12: val12,
		}
		data = append(data, &d)
	}
	return data
}

func BenchmarkSimpleQueueEnqueueDequeue10(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/single/10/ffq"
	data := createData(10)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
		)
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
}

func BenchmarkSimpleQueueEnqueueDequeue100(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/single/100/ffq"
	data := createData(100)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
		)
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
}

func BenchmarkSimpleQueueEnqueueDequeue1000(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/single/1000/ffq"
	data := createData(1000)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
		)
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
}

func BenchmarkSimpleQueueEnqueueDequeue10000(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/single/10000/ffq"
	data := createData(10000)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
		)
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
}

func BenchmarkSimpleQueueBulkEnqueueDequeue10(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/bulk/10/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(10)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
}

func BenchmarkSimpleQueueBulkEnqueueDequeue100(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/bulk/100/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(100)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
}

func BenchmarkSimpleQueueBulkEnqueueDequeue1000(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/bulk/1000/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(1000)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
}

func BenchmarkSimpleQueueBulkEnqueueDequeue10000(b *testing.B) {
	dir := "testdata/benchmark/simple_queue/bulk/10000/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(10000)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, _ := ffq.NewQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
}

func BenchmarkGroupQueueEnqueueDequeue10_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/single/10/ffq"
	data := createData(10)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				mc, err := gq.Dequeue(batch)
				if err != nil {
					return
				}
				for m := range mc {
					gq.UpdateIndex(m)
					total--
				}
			}
			gq.CloseIndex(10 * time.Microsecond)
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueEnqueueDequeue100_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/single/100/ffq"
	data := createData(100)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				mc, err := gq.Dequeue(batch)
				if err != nil {
					return
				}
				for m := range mc {
					gq.UpdateIndex(m)
					total--
				}
			}
			gq.CloseIndex(10 * time.Microsecond)
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueEnqueueDequeue1000_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/single/1000/ffq"
	data := createData(1000)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				mc, err := gq.Dequeue(batch)
				if err != nil {
					return
				}
				for m := range mc {
					gq.UpdateIndex(m)
					total--
				}
			}
			gq.CloseIndex(10 * time.Microsecond)
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueEnqueueDequeue10000_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/single/10000/ffq"
	data := createData(10000)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				mc, err := gq.Dequeue(batch)
				if err != nil {
					return
				}
				for m := range mc {
					gq.UpdateIndex(m)
					total--
				}
			}
			gq.CloseIndex(10 * time.Microsecond)
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueBulkEnqueueDequeue10_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/bulk/10/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(10)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				msc, err := gq.BulkDequeue(batch, size, lazy)
				if err != nil {
					return
				}
				for ms := range msc {
					if len(ms) > 0 {
						gq.UpdateIndex(ms[len(ms)-1])
					}
					total -= len(ms)
				}
			}
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueBulkEnqueueDequeue100_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/bulk/100/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(100)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				msc, err := gq.BulkDequeue(batch, size, lazy)
				if err != nil {
					return
				}
				for ms := range msc {
					if len(ms) > 0 {
						gq.UpdateIndex(ms[len(ms)-1])
					}
					total -= len(ms)
				}
			}
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueBulkEnqueueDequeue1000_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/bulk/1000/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(1000)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				msc, err := gq.BulkDequeue(batch, size, lazy)
				if err != nil {
					return
				}
				for ms := range msc {
					if len(ms) > 0 {
						gq.UpdateIndex(ms[len(ms)-1])
					}
					total -= len(ms)
				}
			}
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}

func BenchmarkGroupQueueBulkEnqueueDequeue10000_3Group(b *testing.B) {
	dir := "testdata/benchmark/group_queue/bulk/10000/ffq"
	size := 100
	lazy := 10 * time.Millisecond
	data := createData(10000)
	testQueues := []string{"queue1", "queue2", "queue3"}
	batch := 10
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		total := len(data) * len(testQueues)
		gq, _ := ffq.NewGroupQueue[BenchmarkData](
			"benchmark",
			ffq.WithFileDir(dir),
			ffq.WithQueueSize(1000),
			ffq.WithEnqueueWriteSize(15),
			ffq.WithPageSize(3),
			ffq.WithDataFixedLength(4),
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
			for 0 < total {
				msc, err := gq.BulkDequeue(batch, size, lazy)
				if err != nil {
					return
				}
				for ms := range msc {
					if len(ms) > 0 {
						gq.UpdateIndex(ms[len(ms)-1])
					}
					total -= len(ms)
				}
			}
		}(&wg)
		wg.Wait()
		b.StopTimer()

		os.RemoveAll(dir)
	}
}
