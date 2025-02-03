package ffq

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestData struct {
	Name  string
	Value int
}

func createBulkData(start int, size int) []*TestData {
	d := make([]*TestData, 0, size)
	for i := start; i < size+start; i++ {
		d = append(d, &TestData{Value: i})
	}
	return d
}

func readQueueFile(f io.Reader) ([]*TestData, error) {
	var testData []*TestData
	reader := bufio.NewReader(f)
	for {
		b, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		var items []*TestData
		err = json.Unmarshal(b, &items)
		if err != nil {
			return nil, err
		}
		testData = append(testData, items...)
	}
	return testData, nil
}

func TestNewQueue_withOptions(t *testing.T) {
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

			q, err := NewQueue[TestData]("testQueue", tt.opts...)
			if (err == nil) != tt.wantExpectNoError {
				t.Errorf("unexpected error state: %v", err)
			}
			if err != nil {
				return
			}
			defer q.CloseQueue()
			defer q.CloseIndex()

			q.WaitInitialize()

			if q.Length() != 0 {
				t.Errorf("new queue length should be 0, but got %d", q.Length())
			}

			if q.size != tt.wantSize {
				t.Errorf("size got = %d, want = %d", q.size, tt.wantSize)
			}
			if q.maxPage != tt.wantMaxPage {
				t.Errorf("maxPage got = %d, want = %d", q.maxPage, tt.wantMaxPage)
			}
		})
	}
}

func TestNewQueue_withInitialize(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
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
			dequeueNums:     8,
			wantTail:        15,
			wantCurrentPage: 1,
		},
		{
			name:            "with page reset 0",
			enqueueNums:     25,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     18,
			wantTail:        5,
			wantCurrentPage: 0,
		},
		{
			name:            "just rotate round 1",
			enqueueNums:     10,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     5,
			wantTail:        10,
			wantCurrentPage: 1,
		},
		{
			name:            "just rotate round 2",
			enqueueNums:     30,
			bulkEnqueueNums: 0,
			bulkSize:        0,
			dequeueNums:     25,
			wantTail:        10,
			wantCurrentPage: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			// preparation
			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := q.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.enqueueNums; i++ {
					err := q.Enqueue(&TestData{Value: i})
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				for i := 0; i < tt.bulkEnqueueNums; i += tt.bulkSize {
					items := createBulkData(i, tt.bulkSize)
					err := q.BulkEnqueue(items)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

			index := readIndex(filepath.Join(dir, indexFilename))
			if tt.dequeueNums == 0 && index != nil {
				t.Errorf("index got is not nil, %d", *index)
			}
			if tt.dequeueNums > 0 && *index != uint64((tt.dequeueNums-1)%int(q.size*q.maxPage)) {
				t.Errorf("index got = %d, want = %d", *index, tt.dequeueNums-1)
			}
			q, err = NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(uint64(queueSize)), WithMaxPage(uint64(maxPage)))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}
			q.WaitInitialize()
			if q.currentPage != tt.wantCurrentPage {
				t.Errorf("currentPage got = %d, want = %d", q.currentPage, tt.wantCurrentPage)
			}
			if q.tail != tt.wantTail {
				t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
			}
			q.CloseQueue()
			q.CloseIndex()
		})
	}
}

func TestEnqueue_SPSC(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	tests := []struct {
		name        string
		enqueueNums int
		dequeueNums int
		wantTail    uint64
	}{
		{
			name:        "simple enqueue",
			enqueueNums: 8,
			dequeueNums: 0,
			wantTail:    8,
		},
		{
			name:        "with file rotate",
			enqueueNums: 15,
			dequeueNums: 8,
			wantTail:    15,
		},
		{
			name:        "tail reset 0",
			enqueueNums: 25,
			dequeueNums: 18,
			wantTail:    5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := q.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			testData := make([]*TestData, 0, tt.enqueueNums)
			queuedData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}
			var currentPage uint64 = 0
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i, d := range testData {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
					if currentPage != q.currentPage || (i+1) == tt.enqueueNums {
						f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						items, err := readQueueFile(f)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						queuedData = append(queuedData, items...)
						currentPage++
						if currentPage == maxPage {
							currentPage = 0
						}
					}
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

			if q.tail != tt.wantTail {
				t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
			}
			if len(queuedData) != len(testData) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData), len(testData))
			}
			for i := 0; i < tt.enqueueNums; i++ {
				if *queuedData[i] != *testData[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData[i], testData[i])
				}
			}
		})
	}
}

func TestEnqueue_MPSC(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	tests := []struct {
		name         string
		enqueue1Nums int
		enqueue2Nums int
		dequeueNums  int
		wantTail     uint64
	}{
		{
			name:         "simple enqueue",
			enqueue1Nums: 5,
			enqueue2Nums: 3,
			dequeueNums:  0,
			wantTail:     8,
		},
		{
			name:         "with file rotate",
			enqueue1Nums: 8,
			enqueue2Nums: 7,
			dequeueNums:  8,
			wantTail:     15,
		},
		{
			name:         "tail reset 0",
			enqueue1Nums: 13,
			enqueue2Nums: 12,
			dequeueNums:  18,
			wantTail:     5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(MPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := q.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(2)
			testData1 := make([]*TestData, 0, tt.enqueue1Nums)
			queuedData1 := make([]*TestData, 0, tt.enqueue1Nums)
			for i := 0; i < tt.enqueue1Nums; i++ {
				testData1 = append(testData1, &TestData{Name: "1", Value: i})
			}
			testData2 := make([]*TestData, 0, tt.enqueue2Nums)
			queuedData2 := make([]*TestData, 0, tt.enqueue2Nums)
			for i := 0; i < tt.enqueue2Nums; i++ {
				testData2 = append(testData2, &TestData{Name: "2", Value: i})

			}
			var currentPage uint64 = 0
			var pmu sync.Mutex
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData1 {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					if cp := atomic.LoadUint64(&currentPage); cp != atomic.LoadUint64(&q.currentPage) {
						pmu.Lock()
						if cp == atomic.LoadUint64(&currentPage) {
							f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, cp)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							for _, item := range items {
								if item.Name == "1" {
									queuedData1 = append(queuedData1, item)
								}
								if item.Name == "2" {
									queuedData2 = append(queuedData2, item)
								}
							}
							p := atomic.AddUint64(&currentPage, 1)
							if p == maxPage {
								atomic.StoreUint64(&currentPage, 0)
							}
						}
						pmu.Unlock()
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
			}(&wg)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData2 {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					if cp := atomic.LoadUint64(&currentPage); cp != atomic.LoadUint64(&q.currentPage) {
						pmu.Lock()
						if cp == atomic.LoadUint64(&currentPage) {
							f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, cp)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							for _, item := range items {
								if item.Name == "1" {
									queuedData1 = append(queuedData1, item)
								}
								if item.Name == "2" {
									queuedData2 = append(queuedData2, item)
								}
							}
							p := atomic.AddUint64(&currentPage, 1)
							if p == maxPage {
								atomic.StoreUint64(&currentPage, 0)
							}
						}
						pmu.Unlock()
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
			}(&wg)

			wg.Wait()

			f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
			}
			items, err := readQueueFile(f)
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
			}
			for _, item := range items {
				if item.Name == "1" {
					queuedData1 = append(queuedData1, item)
				}
				if item.Name == "2" {
					queuedData2 = append(queuedData2, item)
				}
			}

			err = q.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			if q.tail != tt.wantTail {
				t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
			}
			if len(queuedData1) != len(testData1) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData1), len(testData1))
			}
			for i := 0; i < tt.enqueue1Nums; i++ {
				if *queuedData1[i] != *testData1[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData1[i], testData1[i])
				}
			}
			if len(queuedData2) != len(testData2) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData2), len(testData2))
			}
			for i := 0; i < tt.enqueue2Nums; i++ {
				if *queuedData2[i] != *testData2[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData2[i], testData2[i])
				}
			}
		})
	}
}

func TestBulkEnqueue_SPSC(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
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
			dequeueNums: 0,
			wantTail:    8,
		},
		{
			name:        "with file rotate",
			enqueueNums: 15,
			bulkSize:    7,
			dequeueNums: 8,
			wantTail:    15,
		},
		{
			name:        "tail reset 0",
			enqueueNums: 25,
			bulkSize:    12,
			dequeueNums: 18,
			wantTail:    5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := q.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			testData := make([]*TestData, 0, tt.enqueueNums)
			queuedData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}
			var currentPage uint64 = 0
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				bulkTestData := []*TestData{}
				for i, d := range testData {
					bulkTestData = append(bulkTestData, d)
					if lbt := len(bulkTestData); lbt != 0 && !(lbt%tt.bulkSize == 0 || (i+1) == tt.enqueueNums) {
						continue
					}
					err := q.BulkEnqueue(bulkTestData)
					bulkTestData = []*TestData{}
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
					if currentPage != q.currentPage || (i+1) == tt.enqueueNums {
						f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						items, err := readQueueFile(f)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						queuedData = append(queuedData, items...)
						currentPage++
						if currentPage == maxPage {
							currentPage = 0
						}
					}
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

			if q.tail != tt.wantTail {
				t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
			}
			if len(queuedData) != len(testData) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData), len(testData))
			}
			for i := 0; i < tt.enqueueNums; i++ {
				if *queuedData[i] != *testData[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData[i], testData[i])
				}
			}
		})

	}
}

func TestBulkEnqueue_MPSC(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
	tests := []struct {
		name         string
		enqueue1Nums int
		enqueue2Nums int
		bulkSize     int
		dequeueNums  int
		wantTail     uint64
	}{
		{
			name:         "simple enqueue",
			enqueue1Nums: 5,
			enqueue2Nums: 3,
			bulkSize:     2,
			dequeueNums:  0,
			wantTail:     8,
		},
		{
			name:         "with file rotate",
			enqueue1Nums: 8,
			enqueue2Nums: 7,
			bulkSize:     5,
			dequeueNums:  8,
			wantTail:     15,
		},
		{
			name:         "tail reset 0",
			enqueue1Nums: 13,
			enqueue2Nums: 12,
			bulkSize:     7,
			dequeueNums:  18,
			wantTail:     5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := os.MkdirTemp("", "ffqtest")
			defer removeAll(dir, t)

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(MPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for i := 0; i < tt.dequeueNums; i++ {
					m, err := q.Dequeue()
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(2)
			testData1 := make([]*TestData, 0, tt.enqueue1Nums)
			queuedData1 := make([]*TestData, 0, tt.enqueue1Nums)
			for i := 0; i < tt.enqueue1Nums; i++ {
				testData1 = append(testData1, &TestData{Name: "1", Value: i})
			}
			testData2 := make([]*TestData, 0, tt.enqueue2Nums)
			queuedData2 := make([]*TestData, 0, tt.enqueue2Nums)
			for i := 0; i < tt.enqueue2Nums; i++ {
				testData2 = append(testData2, &TestData{Name: "2", Value: i})

			}
			var currentPage uint64 = 0
			var pmu sync.Mutex
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				bulkTestData := []*TestData{}
				for i, d := range testData1 {
					bulkTestData = append(bulkTestData, d)
					if lbt := len(bulkTestData); lbt != 0 && !(lbt%tt.bulkSize == 0 || (i+1) == tt.enqueue1Nums) {
						continue
					}
					err := q.BulkEnqueue(bulkTestData)
					bulkTestData = []*TestData{}
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					if cp := atomic.LoadUint64(&currentPage); cp != atomic.LoadUint64(&q.currentPage) {
						pmu.Lock()
						if cp == atomic.LoadUint64(&currentPage) {
							f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, cp)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							for _, item := range items {
								if item.Name == "1" {
									queuedData1 = append(queuedData1, item)
								}
								if item.Name == "2" {
									queuedData2 = append(queuedData2, item)
								}
							}
							p := atomic.AddUint64(&currentPage, 1)
							if p == maxPage {
								atomic.StoreUint64(&currentPage, 0)
							}
						}
						pmu.Unlock()
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
			}(&wg)

			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				bulkTestData := []*TestData{}
				for i, d := range testData2 {
					bulkTestData = append(bulkTestData, d)
					if lbt := len(bulkTestData); lbt != 0 && !(lbt%tt.bulkSize == 0 || (i+1) == tt.enqueue2Nums) {
						continue
					}
					err := q.BulkEnqueue(bulkTestData)
					bulkTestData = []*TestData{}
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					if cp := atomic.LoadUint64(&currentPage); cp != atomic.LoadUint64(&q.currentPage) {
						pmu.Lock()
						if cp == atomic.LoadUint64(&currentPage) {
							f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, cp)))
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							items, err := readQueueFile(f)
							if err != nil {
								t.Errorf("unexpected error state: %v", err)
							}
							for _, item := range items {
								if item.Name == "1" {
									queuedData1 = append(queuedData1, item)
								}
								if item.Name == "2" {
									queuedData2 = append(queuedData2, item)
								}
							}
							p := atomic.AddUint64(&currentPage, 1)
							if p == maxPage {
								atomic.StoreUint64(&currentPage, 0)
							}
						}
						pmu.Unlock()
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
			}(&wg)

			wg.Wait()

			f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, currentPage)))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
			}
			items, err := readQueueFile(f)
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
			}
			for _, item := range items {
				if item.Name == "1" {
					queuedData1 = append(queuedData1, item)
				}
				if item.Name == "2" {
					queuedData2 = append(queuedData2, item)
				}
			}

			err = q.CloseQueue()
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			if q.tail != tt.wantTail {
				t.Errorf("tail got = %d, want = %d", q.tail, tt.wantTail)
			}
			if len(queuedData1) != len(testData1) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData1), len(testData1))
			}
			for i := 0; i < tt.enqueue1Nums; i++ {
				if *queuedData1[i] != *testData1[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData1[i], testData1[i])
				}
			}
			if len(queuedData2) != len(testData2) {
				t.Errorf("queued length got = %d, want = %d", len(queuedData2), len(testData2))
			}
			for i := 0; i < tt.enqueue2Nums; i++ {
				if *queuedData2[i] != *testData2[i] {
					t.Errorf("queued data got = %v, want = %v", queuedData2[i], testData2[i])
				}
			}
		})
	}
}

func TestDequeue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
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

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					m, err := q.Dequeue()
					if err != nil {
						if IsErrQueueClose(err) {
							break
						}
						t.Errorf("unexpected error state: %v", err)
					}
					q.UpdateIndex(m)
					index := readIndex(q.indexFile.Name())
					if *index != uint64(i) {
						t.Errorf("index got = %d, want = %d", *index, i)
					}
					if *m.Item() != *testData[i] {
						t.Errorf("item got = %v, want = %v", *m.Item(), *testData[i])
					}
					if m.Name() != "testQueue" {
						t.Errorf("message name got = %s, want = %s", m.Name(), "testQueue")
					}
					if m.Index() != uint64(i) {
						t.Errorf("index got = %d, want = %d", m.Index(), i)
					}
					i++
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
				if i != tt.enqueueNums {
					t.Errorf("total dequeue got = %d, want = %d", i+1, tt.enqueueNums)
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

		})
	}
}

func TestBulkDequeue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2
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
			enqueueNums: 8,
			size:        uint64(5),
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

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					ms, err := q.BulkDequeue(tt.size, tt.lazy)
					if err != nil {
						if IsErrQueueClose(err) {
							break
						}
						t.Errorf("unexpected error state: %v", err)
					}
					for _, m := range ms {
						q.UpdateIndex(m)
						index := readIndex(q.indexFile.Name())
						if *index != uint64(i) {
							t.Errorf("index got = %d, want = %d", *index, i)
						}
						if *m.Item() != *testData[i] {
							t.Errorf("item got = %v, want = %v", *m.Item(), *testData[i])
						}
						if m.Name() != "testQueue" {
							t.Errorf("message name got = %s, want = %s", m.Name(), "testQueue")
						}
						if m.Index() != uint64(i) {
							t.Errorf("index got = %d, want = %d", m.Index(), i)
						}
						i++
					}
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
				if i != tt.enqueueNums {
					t.Errorf("total dequeue got = %d, want = %d", i+1, tt.enqueueNums)
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

		})
	}
}

func TestFuncAfterDequeue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2

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

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					err := q.FuncAfterDequeue(tt.f)
					if tt.name == "func error" {
						index := readIndex(q.indexFile.Name())
						if *index != 0 {
							t.Errorf("index got = %d, want = %d", *index, i)
						}
						break
					} else {
						if err != nil {
							if IsErrQueueClose(err) {
								break
							}
							t.Errorf("unexpected error state: %v", err)
						}
						index := readIndex(q.indexFile.Name())
						if *index != uint64(i) {
							t.Errorf("index got = %d, want = %d", *index, i)
						}
					}
					i++
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
				if tt.name != "func error" && i != tt.enqueueNums {
					t.Errorf("total dequeue got = %d, want = %d", i+1, tt.enqueueNums)
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

		})
	}
}

func TestFuncAfterBulkDequeue(t *testing.T) {
	var queueSize uint64 = 10
	var maxPage uint64 = 2

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

			q, err := NewQueue[TestData]("testQueue", WithFileDir(dir), WithQueueSize(queueSize), WithMaxPage(maxPage), WithQueueType(SPSC))
			if err != nil {
				t.Errorf("unexpected error state: %v", err)
				return
			}

			testData := make([]*TestData, 0, tt.enqueueNums)
			for i := 0; i < tt.enqueueNums; i++ {
				testData = append(testData, &TestData{Value: i})
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					n, err := q.FuncAfterBulkDequeue(tt.size, tt.lazy, tt.f)
					if tt.name == "func error" {
						index := readIndex(q.indexFile.Name())
						if *index != uint64(n-1) {
							t.Errorf("index got = %d, want = %d", *index, uint64(n-1))
						}
						break
					} else {
						if err != nil {
							if IsErrQueueClose(err) {
								break
							}
							t.Errorf("unexpected error state: %v", err)
						}
						index := readIndex(q.indexFile.Name())
						if *index != uint64(i+n-1) {
							t.Errorf("index got = %d, want = %d", *index, uint64(i+n-1))
						}
					}
					i += n
				}
				err := q.CloseIndex()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
				if tt.name != "func error" && i != tt.enqueueNums {
					t.Errorf("total dequeue got = %d, want = %d", i+1, tt.enqueueNums)
				}
			}(&wg)

			q.WaitInitialize()

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, d := range testData {
					err := q.Enqueue(d)
					if err != nil {
						t.Errorf("unexpected error state: %v", err)
					}
					// Forcing the file to sleep
					// because the process is too fast and the file time does not change.
					time.Sleep(10 * time.Microsecond)
				}
				err := q.CloseQueue()
				if err != nil {
					t.Errorf("unexpected error state: %v", err)
					return
				}
			}(&wg)

			wg.Wait()

		})
	}
}
