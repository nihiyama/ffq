package ffq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewGroupQueue_withOptions(t *testing.T) {
	tests := []struct {
		name              string
		opts              []Option
		wantSize          uint64
		wantMaxPage       uint64
		wantQueueType     QueueType
		wantExpectNoError bool
	}{
		{
			name:              "no options",
			opts:              nil,
			wantSize:          1024,
			wantMaxPage:       2,
			wantExpectNoError: true,
		},
		{
			name: "set all options",
			opts: []Option{
				WithQueueSize(10),
				WithMaxPage(5),
				WithEncoder(json.Marshal),
				WithDecoder(json.Unmarshal),
				WithQueueType(MPSC),
				WithGroupSize(10),
			},
			wantSize:          10,
			wantMaxPage:       5,
			wantExpectNoError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			tt.opts = append(tt.opts, WithFileDir(dir))

			gq, err := NewGroupQueue[TestData]("testQueue", tt.opts...)
			if (err == nil) != tt.wantExpectNoError {
				t.Errorf("unexpected error state: %v", err)
			}
			if err != nil {
				return
			}
			defer gq.CloseQueue()
			defer gq.CloseIndex()

			gq.WaitInitialize()

			if _, total := gq.Length(); total != 0 {
				t.Errorf("new queue length should be 0, but got %d", total)
			}

			if gq.size != tt.wantSize {
				t.Errorf("size got = %d, want = %d", gq.size, tt.wantSize)
			}
			if gq.maxPage != tt.wantMaxPage {
				t.Errorf("maxPage got = %d, want = %d", gq.maxPage, tt.wantMaxPage)
			}
		})
	}
}

func TestNewGroupQueue_withInitialize(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	tests := []struct {
		name            string
		enqueueNums     int
		bulkEnqueueNums int
		bulkSize        int
		dequeueNums     int
		wantTail        uint64
		wantCurrentPage uint64
	}{
		{
			name:            "no queue",
			enqueueNums:     0,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     0,
			wantTail:        0,
			wantCurrentPage: 0,
		},
		{
			name:            "with enqueue only",
			enqueueNums:     5,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     0,
			wantTail:        5,
			wantCurrentPage: 0,
		},
		{
			name:            "with enqueue and bulk enqueue",
			enqueueNums:     3,
			bulkEnqueueNums: 6,
			bulkSize:        3,
			dequeueNums:     0,
			wantTail:        9,
			wantCurrentPage: 0,
		},
		{
			name:            "with file rotate",
			enqueueNums:     5,
			bulkEnqueueNums: 10,
			bulkSize:        10,
			dequeueNums:     8 * len(queueNames),
			wantTail:        15,
			wantCurrentPage: 1,
		},
		{
			name:            "with page reset 0",
			enqueueNums:     25,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     18 * len(queueNames),
			wantTail:        5,
			wantCurrentPage: 0,
		},
		{
			name:            "just rotate round 1",
			enqueueNums:     10,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     5 * len(queueNames),
			wantTail:        10,
			wantCurrentPage: 1,
		},
		{
			name:            "just rotate round 2",
			enqueueNums:     30,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     25 * len(queueNames),
			wantTail:        10,
			wantCurrentPage: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			wantIndexMap := map[string]uint64{}

			// preparation
			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := gq.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					gq.UpdateIndex(m)
					wantIndexMap[m.Name()] = m.Index()
				}
			}(&dwg)

			gq.WaitInitialize()

			// for enqueue
			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					for i := 0; i < tt.enqueueNums; i++ {
						err := gq.Enqueue(qn, &TestData{Value: i})
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
					for i := 0; i < tt.bulkEnqueueNums; i += tt.bulkSize {
						items := createBulkData(i, tt.bulkSize)
						err := gq.BulkEnqueue(qn, items)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			for _, qn := range queueNames {
				index := readIndex(filepath.Join(dir, qn, indexFilename))
				if tt.dequeueNums == 0 && index != nil {
					t.Errorf("index got is not nil, %d", *index)
				}
				if tt.dequeueNums > 0 && *index != wantIndexMap[qn] {
					t.Errorf("index got = %d, want = %d", *index, wantIndexMap[qn])
				}
			}

			gq, err = NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(uint64(queueSize)), WithMaxPage(uint64(maxPage)))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			gq.WaitInitialize()

			for _, qn := range queueNames {
				q, err := gq.getQueue(qn)
				if err != nil {
					// no enqueue
					continue
				}
				if q.currentPage != tt.wantCurrentPage {
					t.Errorf("currentPage got = %d, want = %d", q.currentPage, tt.wantCurrentPage)
				}
				if q.tail != tt.wantTail {
					t.Errorf("%s tail got = %d, want = %d", qn, q.tail, tt.wantTail)
				}
			}

			gq.CloseQueue()
			gq.CloseIndex()
		})
	}
}

func TestGroupEnqueue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	tests := []struct {
		name        string
		enqueueNums int
		dequeueNums int
		wantTail    uint64
	}{
		{
			name:        "simple enqueue",
			enqueueNums: 8,
			dequeueNums: 0 * len(queueNames),
			wantTail:    8,
		},
		{
			name:        "with file rotate",
			enqueueNums: 15,
			dequeueNums: 8 * len(queueNames),
			wantTail:    15,
		},
		{
			name:        "tail reset 0",
			enqueueNums: 25,
			dequeueNums: 18 * len(queueNames),
			wantTail:    5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup

			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := gq.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					gq.UpdateIndex(m)
				}
			}(&dwg)

			gq.WaitInitialize()

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}
			queuedData := sync.Map{}
			for _, qn := range queueNames {
				qd := make([]*TestData, 0, tt.enqueueNums)
				queuedData.Store(qn, qd)
			}

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					var currentPage uint64 = 0
					for i, d := range testData {
						err := gq.Enqueue(qn, d)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
						q, _ := gq.getQueue(qn)
						if currentPage != q.currentPage || (i+1) == tt.enqueueNums {
							f, err := os.Open(filepath.Join(dir, qn, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							qd, _ := queuedData.Load(qn)
							qd = append(qd.([]*TestData), items...)
							queuedData.Store(qn, qd)
							currentPage++
							if currentPage == maxPage {
								currentPage = 0
							}
						}
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			for _, qn := range queueNames {
				q, err := gq.getQueue(qn)
				if err != nil {
					// no enqueue
					continue
				}
				if q.tail != tt.wantTail {
					t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
				}
				qdAny, _ := queuedData.Load(qn)
				qd := qdAny.([]*TestData)
				if len(qd) != len(testData) {
					t.Errorf("queued length got = %d, want = %d", len(qd), len(testData))
				}
				for i := 0; i < tt.enqueueNums; i++ {
					if *qd[i] != *testData[i] {
						t.Errorf("queued data got = %v, want = %v", qd[i], testData[i])
					}
				}
			}
		})
	}
}

func TestGroupBulkEnqueue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	tests := []struct {
		name        string
		enqueueNums int
		bulkSize    int
		dequeueNums int
		wantTail    uint64
	}{
		{
			name:        "simple enqueue",
			enqueueNums: 8,
			bulkSize:    5,
			dequeueNums: 0 * len(queueNames),
			wantTail:    8,
		},
		{
			name:        "with file rotate",
			enqueueNums: 15,
			bulkSize:    7,
			dequeueNums: 8 * len(queueNames),
			wantTail:    15,
		},
		{
			name:        "tail reset 0",
			enqueueNums: 25,
			bulkSize:    12,
			dequeueNums: 18 * len(queueNames),
			wantTail:    5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := gq.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					gq.UpdateIndex(m)
				}
			}(&dwg)

			gq.WaitInitialize()

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}
			queuedData := sync.Map{}
			for _, qn := range queueNames {
				qd := make([]*TestData, 0, tt.enqueueNums)
				queuedData.Store(qn, qd)
			}

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					var currentPage uint64 = 0
					bulkTestData := []*TestData{}
					for i, d := range testData {
						bulkTestData = append(bulkTestData, d)
						if lbt := len(bulkTestData); lbt != 0 && !(lbt%tt.bulkSize == 0 || (i+1) == tt.enqueueNums) {
							continue
						}
						err := gq.BulkEnqueue(qn, bulkTestData)
						bulkTestData = []*TestData{}
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
						q, _ := gq.getQueue(qn)
						if currentPage != q.currentPage || (i+1) == tt.enqueueNums {
							f, err := os.Open(filepath.Join(dir, qn, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							qd, _ := queuedData.Load(qn)
							qd = append(qd.([]*TestData), items...)
							queuedData.Store(qn, qd)
							currentPage++
							if currentPage == maxPage {
								currentPage = 0
							}
						}
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			for _, qn := range queueNames {
				q, err := gq.getQueue(qn)
				if err != nil {
					// no enqueue
					continue
				}
				if q.tail != tt.wantTail {
					t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
				}
				qdAny, _ := queuedData.Load(qn)
				qd := qdAny.([]*TestData)
				if len(qd) != len(testData) {
					t.Errorf("queued length got = %d, want = %d", len(qd), len(testData))
				}
				for i := 0; i < tt.enqueueNums; i++ {
					if *qd[i] != *testData[i] {
						t.Errorf("queued data got = %v, want = %v", qd[i], testData[i])
					}
				}
			}
		})

	}
}

func TestGroupDequeue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	tests := []struct {
		name        string
		enqueueNums int
	}{
		{
			name:        "simple dequeue",
			enqueueNums: 15,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					m, err := gq.Dequeue()
					if err != nil {
						if IsErrQueueClose(err) {
							break
						}
						t.Errorf("unexpected error state: %v", err)
					}
					gq.UpdateIndex(m)
					q, err := gq.getQueue(m.Name())
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					index := readIndex(q.indexFile.Name())
					if *index != uint64(m.Index()) {
						t.Errorf("index got = %d, want = %d", *index, m.Index())
					}
				}
			}(&dwg)

			gq.WaitInitialize()

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					for _, d := range testData {
						err := gq.Enqueue(qn, d)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
		})
	}
}

func TestGroupBulkDequeue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	tests := []struct {
		name        string
		enqueueNums int
		size        uint64
		lazy        time.Duration
	}{
		{
			name:        "simple dequeue",
			enqueueNums: 8,
			size:        uint64(5),
			lazy:        1 * time.Millisecond,
		},
		{
			name:        "reach size",
			enqueueNums: 15,
			size:        uint64(7),
			lazy:        300 * time.Millisecond,
		},
		{
			name:        "reach timer",
			enqueueNums: 10,
			size:        uint64(10),
			lazy:        5 * time.Nanosecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					ms, err := gq.BulkDequeue(tt.size, tt.lazy)
					if err != nil {
						if IsErrQueueClose(err) {
							break
						}
						t.Errorf("unexpected error state: %v", err)
					}
					for _, m := range ms {
						gq.UpdateIndex(m)
						q, err := gq.getQueue(m.Name())
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						index := readIndex(q.indexFile.Name())
						if *index != uint64(m.Index()) {
							t.Errorf("index got = %d, want = %d", *index, m.Index())
						}
					}
				}
			}(&dwg)

			gq.WaitInitialize()

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					for _, d := range testData {
						err := gq.Enqueue(qn, d)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

		})
	}
}

func TestGroupFuncAfterDequeue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3

	normalF := func(td *TestData) error {
		return nil
	}
	errorF := func(td *TestData) error {
		return fmt.Errorf("error")
	}

	tests := []struct {
		name        string
		enqueueNums int
		f           func(*TestData) error
	}{
		{
			name:        "simple dequeue",
			enqueueNums: 8,
			f:           normalF,
		},
		{
			name:        "func error",
			enqueueNums: 8,
			f:           errorF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					err := gq.FuncAfterDequeue(tt.f)
					if tt.name == "func error" {
						// nothing to do
						break
					} else {
						if err != nil {
							if IsErrQueueClose(err) {
								break
							}
							t.Errorf("unexpected error state: %v", err)
						}
					}
				}
			}(&dwg)

			gq.WaitInitialize()

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					for _, d := range testData {
						err := gq.Enqueue(qn, d)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
				}(&ewg, qn)
			}

			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			if tt.name != "func error" {
				for _, qn := range queueNames {
					index := readIndex(filepath.Join(dir, qn, indexFilename))
					if want := uint64(tt.enqueueNums - 1); *index != want {
						t.Errorf("index got = %d, want = %d", *index, want)
					}
				}
			}
		})
	}
}

func TestGroupFuncAfterBulkDequeue(t *testing.T) {
	queueNames := []string{"0", "1", "2"}
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3

	normalF := func(td []*TestData) error {
		return nil
	}
	errorF := func(td []*TestData) error {
		return fmt.Errorf("error")
	}

	tests := []struct {
		name        string
		enqueueNums int
		size        uint64
		lazy        time.Duration
		f           func([]*TestData) error
	}{
		{
			name:        "simple dequeue",
			enqueueNums: 8,
			size:        uint64(5),
			lazy:        1 * time.Millisecond,
			f:           normalF,
		},
		{
			name:        "reach size",
			enqueueNums: 8,
			size:        uint64(5),
			lazy:        300 * time.Millisecond,
			f:           normalF,
		},
		{
			name:        "reach timer",
			enqueueNums: 8,
			size:        uint64(10),
			lazy:        5 * time.Nanosecond,
			f:           normalF,
		},
		{
			name:        "func error",
			enqueueNums: 8,
			size:        uint64(5),
			lazy:        1 * time.Millisecond,
			f:           errorF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var ewg sync.WaitGroup
			var dwg sync.WaitGroup
			dwg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					_, err := gq.FuncAfterBulkDequeue(tt.size, tt.lazy, tt.f)
					if tt.name == "func error" {
						// nothing to do
						break
					} else {
						if err != nil {
							if IsErrQueueClose(err) {
								break
							}
							t.Errorf("unexpected error state: %v", err)
						}
					}
				}
			}(&dwg)

			gq.WaitInitialize()

			for _, qn := range queueNames {
				ewg.Add(1)
				go func(wg *sync.WaitGroup, qn string) {
					defer wg.Done()
					for _, d := range testData {
						err := gq.Enqueue(qn, d)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						// Forcing the file to sleep and other queues
						// because the process is too fast and the file time does not change.
						time.Sleep(20 * time.Millisecond)
					}
				}(&ewg, qn)
			}
			ewg.Wait()
			err = gq.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			dwg.Wait()
			err = gq.CloseIndex()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			if tt.name != "func error" {
				for _, qn := range queueNames {
					index := readIndex(filepath.Join(dir, qn, indexFilename))
					if want := uint64(tt.enqueueNums - 1); *index != want {
						t.Errorf("index got = %d, want = %d", *index, want)
					}
				}
			}
		})
	}
}

func TestGroupAddQueue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}

	gq.WaitInitialize()

	err = gq.Enqueue("1", &TestData{Value: 1})
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}
	err = gq.Enqueue("2", &TestData{Value: 2})
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}
	err = gq.Enqueue("3", &TestData{Value: 3})
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}
	err = gq.Enqueue("4", &TestData{Value: 4})
	if err.Error() != "reached group queue max size, 3" {
		t.Errorf("want reached group queue max size, 3")
		return
	}

	gq.CloseQueue()

	err = gq.Enqueue("4", &TestData{Value: 4})
	if err.Error() != "already closed" {
		t.Errorf("want already closed")
		return
	}
}

func TestGroupGetActiveQueue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}

	gq.WaitInitialize()

	gq.Enqueue("1", &TestData{Value: 1})
	gq.Enqueue("2", &TestData{Value: 2})

	_, i := gq.getActiveQueue()
	if i != 0 {
		t.Errorf("active queue index got = %d, want = %d", i, 0)
	}
	_, i = gq.getActiveQueue()
	if i != 1 {
		t.Errorf("active queue index got = %d, want = %d", i, 1)
	}
	_, i = gq.getActiveQueue()
	if i != 0 {
		t.Errorf("active queue index got = %d, want = %d", i, 0)
	}
}

func TestGroupQueueLength(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	var groupSize int = 3
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	gq, err := NewGroupQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithGroupSize(groupSize))
	if err != nil {
		t.Errorf("unexpected error state: %v", err)
		return
	}

	gq.WaitInitialize()

	gq.Enqueue("1", &TestData{Value: 1})
	gq.Enqueue("2", &TestData{Value: 2})

	ls, total := gq.Length()
	wantLs := []uint64{1, 1}
	for i, wl := range wantLs {
		if l := ls[i]; l != wl {
			t.Errorf("queue lengths got = %d, want = %d", l, wl)
		}
	}
	if total != 2 {
		t.Errorf("total got = %d, want = %d", total, 2)
	}
}
