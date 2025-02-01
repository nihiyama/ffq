package ringbuffer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type TestData struct {
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

			// Queue の初期化を待機
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
					if uint64((i+1))%queueSize == 0 || (i+1) == tt.enqueueNums {
						page := i / int(queueSize)
						if page == int(maxPage) {
							page = 0
						}
						f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.%d", queueFilename, page)))
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						td, err := readQueueFile(f)
						if err != nil {
							t.Errorf("unexpected error state: %v", err)
						}
						queuedData = append(queuedData, td...)
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

// func TestEnqueue_MPSC(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple enqueue",
// 		},
// 		{
// 			name: "with file rotate",
// 		},
// 		{
// 			name: "tail reset 0",
// 		},
// 	}
// }

// func TestBulkEnqueue_SPSC(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple enqueue",
// 		},
// 		{
// 			name: "with file rotate",
// 		},
// 		{
// 			name: "tail reset 0",
// 		},
// 	}
// }

// func TestBulkEnqueue_MPSC(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple enqueue",
// 		},
// 		{
// 			name: "with file rotate",
// 		},
// 		{
// 			name: "tail reset 0",
// 		},
// 	}
// }

// func TestDequeue(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple dequeue",
// 		},
// 	}
// }

// func TestBulkDequeue(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple dequeue",
// 		},
// 		{
// 			name: "reach size",
// 		},
// 		{
// 			name: "reach timer",
// 		},
// 	}
// }

// func TestFuncAfterDequeue(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple dequeue",
// 		},
// 		{
// 			name: "func error",
// 		},
// 	}
// }

// func TestFuncAfterBulkDequeu(t *testing.T) {
// 	tests := []struct {
// 		name string
// 	}{
// 		{
// 			name: "simple dequeue",
// 		},
// 		{
// 			name: "reach size",
// 		},
// 		{
// 			name: "reach timer",
// 		},
// 		{
// 			name: "func error",
// 		},
// 	}
// }

// func TestUpdateIndex(t *testing.T) {}

// func TestLength(t *testing.T) {}

// func TestNewQueue(t *testing.T) {
// 	tests := []struct {
// 		name        string
// 		fileDir     string
// 		queueSize   int
// 		maxPages    int
// 		encoder     func(v any) ([]byte, error)
// 		decoder     func(data []byte, v any) error
// 		afterRemove bool
// 	}{
// 		{
// 			name:        "new queue",
// 			fileDir:     "testdata/simple_queue/new_queue/ffq_new",
// 			queueSize:   5,
// 			maxPages:    3,
// 			encoder:     json.Marshal,
// 			decoder:     json.Unmarshal,
// 			afterRemove: true,
// 		},
// 	}

// 	type Data struct {
// 		Value int
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if tt.afterRemove {
// 				defer os.RemoveAll(tt.fileDir)
// 			}
// 			actual, _ := NewQueue[Data]("testQueue",
// 				WithFileDir(tt.fileDir),
// 				WithQueueSize(tt.queueSize),
// 				WithMaxPages(tt.maxPages),
// 				WithEncoder(tt.encoder),
// 				WithDecoder(tt.decoder),
// 			)

// 			if tt.maxPages != actual.maxPages {
// 				t.Errorf("Failed test: maxPages, expect: %v, actual: %v", tt.maxPages, actual.maxPages)
// 			}
// 			if tt.fileDir != actual.fileDir {
// 				t.Errorf("Failed test: fileDir, expect: %v, actual: %v", tt.fileDir, actual.fileDir)
// 			}

// 			// wait initialize
// 			actual.WaitInitialize()

// 			if _, err := os.Stat(filepath.Join(tt.fileDir, indexFilename)); os.IsNotExist(err) {
// 				t.Errorf("index file not created")
// 			}
// 			if _, err := os.Stat(filepath.Join(tt.fileDir, "queue.0")); os.IsNotExist(err) {
// 				t.Errorf("queue file not created")
// 			}

// 			if err := actual.CloseQueue(); err != nil {
// 				t.Errorf("CloseQueue failed: %v", err)
// 			}
// 			if err := actual.CloseIndex(); err != nil {
// 				t.Errorf("CloseIndex failed: %v", err)
// 			}
// 		})
// 	}
// }

// func TestNewQueue_initialize_enqueue(t *testing.T) {
// 	tests := []struct {
// 		name                  string
// 		fileDir               string
// 		queueSize             int
// 		maxPages              int
// 		enqueNum              int
// 		beforeDequeueNum      int
// 		expectHeadGlobalIndex int
// 		expectCurrentPage     int
// 		encoder               func(v any) ([]byte, error)
// 		decoder               func(data []byte, v any) error
// 		afterRemove           bool
// 	}{
// 		{
// 			name:                  "restart",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_restart",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              4,
// 			beforeDequeueNum:      3,
// 			expectHeadGlobalIndex: 4,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "next page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_next_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              5,
// 			beforeDequeueNum:      3,
// 			expectHeadGlobalIndex: 0,
// 			expectCurrentPage:     1,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "file rotate",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_file_rotate",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              7,
// 			beforeDequeueNum:      3,
// 			expectHeadGlobalIndex: 2,
// 			expectCurrentPage:     1,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "equal max page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_equal_max_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              15,
// 			beforeDequeueNum:      13,
// 			expectHeadGlobalIndex: 0,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "over max page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_over_max_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              16,
// 			beforeDequeueNum:      13,
// 			expectHeadGlobalIndex: 1,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 	}

// 	type Data struct {
// 		Value int
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if tt.afterRemove {
// 				defer os.RemoveAll(tt.fileDir)
// 			}
// 			bq, _ := NewQueue[Data]("testQueue",
// 				WithFileDir(tt.fileDir),
// 				WithQueueSize(tt.queueSize),
// 				WithMaxPages(tt.maxPages),
// 				WithEncoder(tt.encoder),
// 				WithDecoder(tt.decoder),
// 			)

// 			// prepare data
// 			bq.WaitInitialize()

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func() {
// 				for i := 0; i < tt.enqueNum; i++ {
// 					bq.Enqueue(&Data{Value: i})
// 				}
// 				bq.CloseQueue()
// 				wg.Done()
// 			}()

// 			for j := 0; j < tt.beforeDequeueNum; j++ {
// 				m, _ := bq.Dequeue()
// 				bq.UpdateIndex(m)
// 			}
// 			bq.CloseIndex()
// 			wg.Wait()
// 			aq, _ := NewQueue[Data]("testQueue",
// 				WithFileDir(tt.fileDir),
// 				WithQueueSize(tt.queueSize),
// 				WithMaxPages(tt.maxPages),
// 				WithEncoder(tt.encoder),
// 				WithDecoder(tt.decoder),
// 			)
// 			for j := tt.beforeDequeueNum; j < tt.enqueNum; j++ {
// 				m, _ := aq.Dequeue()
// 				if m.Data().Value != j {
// 					t.Errorf("Failed test: data, expect: %d, actual: %d", j, m.Data().Value)
// 				}
// 				gi, li := m.Index()
// 				if gi != j%tt.queueSize {
// 					t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", j%tt.queueSize, gi)
// 				}
// 				if li != 0 {
// 					t.Errorf("Failed test: localIndex, expect: 0, actual: %d", li)
// 				}
// 				aq.UpdateIndex(m)
// 			}
// 			aq.WaitInitialize()
// 			if aq.currentPage != tt.expectCurrentPage {
// 				t.Errorf("Failed test: page, expect: %d, actual: %d", tt.expectCurrentPage, aq.currentPage)
// 			}
// 			if aq.headGlobalIndex != tt.expectHeadGlobalIndex {
// 				t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", tt.expectHeadGlobalIndex, aq.headGlobalIndex)
// 			}
// 		})
// 	}
// }

// func TestNewQueue_initialize_bulk_enqueue(t *testing.T) {
// 	bulkSize := 3
// 	tests := []struct {
// 		name                  string
// 		fileDir               string
// 		queueSize             int
// 		maxPages              int
// 		enqueNum              int
// 		beforeDequeueNum      int
// 		expectHeadGlobalIndex int
// 		expectCurrentPage     int
// 		encoder               func(v any) ([]byte, error)
// 		decoder               func(data []byte, v any) error
// 		afterRemove           bool
// 	}{
// 		{
// 			name:                  "restart",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_restart",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              4,
// 			beforeDequeueNum:      (3 * bulkSize) - 2,
// 			expectHeadGlobalIndex: 4,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "next page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_next_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              5,
// 			beforeDequeueNum:      (4 * bulkSize) - 2,
// 			expectHeadGlobalIndex: 0,
// 			expectCurrentPage:     1,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "file rotate",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_file_rotate",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              7,
// 			beforeDequeueNum:      (6 * bulkSize) - 2,
// 			expectHeadGlobalIndex: 2,
// 			expectCurrentPage:     1,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "equal max page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_equal_max_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              15,
// 			beforeDequeueNum:      (14 * bulkSize) - 2,
// 			expectHeadGlobalIndex: 0,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 		{
// 			name:                  "over max page",
// 			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_over_max_page",
// 			queueSize:             5,
// 			maxPages:              3,
// 			enqueNum:              16,
// 			beforeDequeueNum:      (15 * bulkSize) - 2,
// 			expectHeadGlobalIndex: 1,
// 			expectCurrentPage:     0,
// 			encoder:               json.Marshal,
// 			decoder:               json.Unmarshal,
// 			afterRemove:           true,
// 		},
// 	}

// 	type Data struct {
// 		Value int
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if tt.afterRemove {
// 				defer os.RemoveAll(tt.fileDir)
// 			}
// 			bq, _ := NewQueue[Data]("testQueue",
// 				WithFileDir(tt.fileDir),
// 				WithQueueSize(tt.queueSize),
// 				WithMaxPages(tt.maxPages),
// 				WithEncoder(tt.encoder),
// 				WithDecoder(tt.decoder),
// 			)

// 			// prepare data
// 			bq.WaitInitialize()

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func() {
// 				v := 0
// 				for i := 0; i < tt.enqueNum; i++ {
// 					data := make([]*Data, 0, bulkSize)
// 					for k := 0; k < bulkSize; k++ {
// 						data = append(data, &Data{Value: v})
// 						v++
// 					}
// 					bq.BulkEnqueue(data)
// 				}
// 				bq.CloseQueue()
// 				wg.Done()
// 			}()

// 			for j := 0; j < tt.beforeDequeueNum; j++ {
// 				m, _ := bq.Dequeue()
// 				bq.UpdateIndex(m)
// 			}
// 			bq.CloseIndex()
// 			wg.Wait()
// 			aq, _ := NewQueue[Data]("testQueue",
// 				WithFileDir(tt.fileDir),
// 				WithQueueSize(tt.queueSize),
// 				WithMaxPages(tt.maxPages),
// 				WithEncoder(tt.encoder),
// 				WithDecoder(tt.decoder),
// 			)
// 			for j := tt.beforeDequeueNum; j < tt.enqueNum*bulkSize; j++ {
// 				m, _ := aq.Dequeue()
// 				if m.Data().Value != j {
// 					t.Errorf("Failed test: data, expect: %d, actual: %d", j, m.Data().Value)
// 				}
// 				gi, li := m.Index()
// 				if gi != j/bulkSize%tt.queueSize {
// 					t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", j/bulkSize%tt.queueSize, gi)
// 				}
// 				if li != j%bulkSize {
// 					t.Errorf("Failed test: localIndex, expect: %d, actual: %d", j%bulkSize, li)
// 				}
// 				aq.UpdateIndex(m)
// 			}
// 			aq.WaitInitialize()
// 			if aq.currentPage != tt.expectCurrentPage {
// 				t.Errorf("Failed test: page, expect: %d, actual: %d", tt.expectCurrentPage, aq.currentPage)
// 			}
// 			if aq.headGlobalIndex != tt.expectHeadGlobalIndex {
// 				t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", tt.expectHeadGlobalIndex, aq.headGlobalIndex)
// 			}
// 		})
// 	}
// }

// func TestQEnqueueDequeue(t *testing.T) {
// 	type Data struct {
// 		Value int
// 	}
// 	tests := []struct {
// 		name            string
// 		enqueueData     []*Data
// 		expectedDequeue []*Data
// 	}{
// 		{
// 			name: "simple enqueue and dequeue",
// 			enqueueData: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			expectedDequeue: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			dir := "testdata/simple_queue/enqueue_dequeue/ffq"
// 			defer os.RemoveAll(dir)

// 			queueSize := 5
// 			maxPages := 2
// 			encoder := json.Marshal
// 			decoder := json.Unmarshal

// 			q, err := NewQueue[Data](
// 				"testQueue",
// 				WithFileDir(dir),
// 				WithQueueSize(queueSize),
// 				WithMaxPages(maxPages),
// 				WithEncoder(encoder),
// 				WithDecoder(decoder),
// 			)

// 			q.WaitInitialize()

// 			if err != nil {
// 				t.Fatalf("failed to create queue: %v", err)
// 			}

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				for _, data := range tt.enqueueData {
// 					err = q.Enqueue(data)
// 					if err != nil {
// 						t.Errorf("enqueue failed: %v", err)
// 					}
// 				}
// 				err = q.CloseQueue()
// 				if err != nil {
// 					t.Errorf("failed to close queue: %v", err)
// 				}
// 			}(&wg)

// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				i := 0
// 				for {
// 					message, err := q.Dequeue()
// 					if IsErrQueueClose(err) {
// 						break
// 					}
// 					if err != nil {
// 						t.Errorf("dequeue failed: %v", err)
// 					} else if tt.expectedDequeue[i].Value != message.Data().Value {
// 						t.Errorf("expected %v, actual %v", tt.expectedDequeue[i].Value, message.Data().Value)
// 					}
// 					message.Index()
// 					q.UpdateIndex(message)
// 					i++
// 				}
// 				err = q.CloseIndex()
// 				if err != nil {
// 					t.Errorf("Failed to close index: %v", err)
// 				}
// 			}(&wg)

// 			wg.Wait()
// 		})
// 	}
// }

// func TestQEnqueueDequeueWithFunc(t *testing.T) {
// 	type Data struct {
// 		Value int
// 	}
// 	tests := []struct {
// 		name            string
// 		enqueueData     []*Data
// 		expectedDequeue []*Data
// 	}{
// 		{
// 			name: "simple enqueue and dequeue",
// 			enqueueData: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			expectedDequeue: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 		},
// 	}
// 	f := func(d *Data) error {
// 		return nil
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			dir := "testdata/simple_queue/enqueue_dequeue_with_func/ffq"
// 			defer os.RemoveAll(dir)

// 			queueSize := 5
// 			maxPages := 2
// 			encoder := json.Marshal
// 			decoder := json.Unmarshal

// 			q, err := NewQueue[Data](
// 				"testQueue",
// 				WithFileDir(dir),
// 				WithQueueSize(queueSize),
// 				WithMaxPages(maxPages),
// 				WithEncoder(encoder),
// 				WithDecoder(decoder),
// 			)

// 			q.WaitInitialize()

// 			if err != nil {
// 				t.Fatalf("failed to create queue: %v", err)
// 			}

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				for _, data := range tt.enqueueData {
// 					err := q.Enqueue(data)
// 					if err != nil {
// 						t.Errorf("enqueue failed: %v", err)
// 					}
// 				}
// 				err = q.CloseQueue()
// 				if err != nil {
// 					t.Errorf("failed to close queue: %v", err)
// 				}
// 			}(&wg)

// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				i := 0
// 				for {
// 					err := q.FuncAfterDequeue(f)
// 					if IsErrQueueClose(err) {
// 						break
// 					}
// 					if err != nil {
// 						t.Errorf("dequeue failed: %v", err)
// 					}
// 					i++
// 				}
// 				err = q.CloseIndex()
// 				if err != nil {
// 					t.Errorf("Failed to close index: %v", err)
// 				}
// 			}(&wg)

// 			wg.Wait()
// 		})
// 	}
// }

// func TestQBulkEnqueueDequeue(t *testing.T) {
// 	type Data struct {
// 		Value int
// 	}
// 	tests := []struct {
// 		name            string
// 		enqueueData     []*Data
// 		expectedDequeue []*Data
// 		bulkSize        int
// 		lazy            time.Duration
// 	}{
// 		{
// 			name: "bulk enqueue and bulk dequeue",
// 			enqueueData: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			expectedDequeue: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			bulkSize: 4,
// 			lazy:     10 * time.Millisecond,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			dir := "testdata/simple_queue/bulk_enqueue_dequeue/ffq"
// 			defer os.RemoveAll(dir)

// 			queueSize := 5
// 			maxPages := 2
// 			encoder := json.Marshal
// 			decoder := json.Unmarshal

// 			q, err := NewQueue[Data](
// 				"testQueue",
// 				WithFileDir(dir),
// 				WithQueueSize(queueSize),
// 				WithMaxPages(maxPages),
// 				WithEncoder(encoder),
// 				WithDecoder(decoder),
// 			)

// 			q.WaitInitialize()

// 			if err != nil {
// 				t.Fatalf("failed to create queue: %v", err)
// 			}

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				err = q.BulkEnqueue(tt.enqueueData)
// 				if err != nil {
// 					t.Errorf("enqueue failed: %v", err)
// 				}
// 				err = q.CloseQueue()
// 				if err != nil {
// 					t.Errorf("failed to close queue: %v", err)
// 				}
// 			}(&wg)

// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				i := 0
// 				for {
// 					messages, err := q.BulkDequeue(tt.bulkSize, tt.lazy)
// 					if err != nil {
// 						if IsErrQueueClose(err) {
// 							err = q.CloseIndex()
// 							if err != nil {
// 								t.Errorf("Failed to close index: %v", err)
// 							}
// 							break
// 						}
// 						t.Errorf("dequeue failed: %v", err)
// 					} else if len(messages) > 0 {
// 						for j := i; j < len(messages); j++ {
// 							if messages[j].Data().Value != tt.expectedDequeue[j].Value {
// 								t.Errorf("expected %v, actual %v", tt.expectedDequeue[j].Value, messages[j].Data().Value)
// 							}
// 							q.UpdateIndex(messages[j])
// 							i++
// 						}
// 					}
// 				}
// 				// next confirm bulkdequeue
// 				_, err = q.BulkDequeue(tt.bulkSize, tt.lazy)
// 				if !IsErrQueueClose(err) {
// 					t.Errorf("Failed test: error, %v", err)
// 				}
// 			}(&wg)

// 			wg.Wait()
// 		})
// 	}
// }

// func TestQBulkEnqueueDequeueWithFunc(t *testing.T) {
// 	type Data struct {
// 		Value int
// 	}
// 	tests := []struct {
// 		name            string
// 		enqueueData     []*Data
// 		expectedDequeue []*Data
// 		bulkSize        int
// 		lazy            time.Duration
// 	}{
// 		{
// 			name: "bulk enqueue and bulk dequeue",
// 			enqueueData: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			expectedDequeue: []*Data{
// 				{Value: 1},
// 				{Value: 2},
// 				{Value: 3},
// 				{Value: 4},
// 				{Value: 5},
// 				{Value: 6},
// 				{Value: 7},
// 				{Value: 8},
// 				{Value: 9},
// 				{Value: 10},
// 				{Value: 11},
// 				{Value: 12},
// 				{Value: 13},
// 				{Value: 14},
// 				{Value: 15},
// 			},
// 			bulkSize: 4,
// 			lazy:     10 * time.Millisecond,
// 		},
// 	}
// 	f := func(d []*Data) error {
// 		return nil
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			dir := "testdata/simple_queue/bulk_enqueue_dequeue/ffq"
// 			defer os.RemoveAll(dir)

// 			queueSize := 5
// 			maxPages := 2
// 			encoder := json.Marshal
// 			decoder := json.Unmarshal

// 			q, err := NewQueue[Data](
// 				"testQueue",
// 				WithFileDir(dir),
// 				WithQueueSize(queueSize),
// 				WithMaxPages(maxPages),
// 				WithEncoder(encoder),
// 				WithDecoder(decoder),
// 			)

// 			q.WaitInitialize()

// 			if err != nil {
// 				t.Fatalf("failed to create queue: %v", err)
// 			}

// 			var wg sync.WaitGroup
// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				err = q.BulkEnqueue(tt.enqueueData)
// 				if err != nil {
// 					t.Errorf("enqueue failed: %v", err)
// 				}
// 				err = q.CloseQueue()
// 				if err != nil {
// 					t.Errorf("failed to close queue: %v", err)
// 				}
// 			}(&wg)

// 			wg.Add(1)
// 			go func(wg *sync.WaitGroup) {
// 				var err error
// 				defer wg.Done()
// 				for {
// 					err := q.FuncAfterBulkDequeue(tt.bulkSize, tt.lazy, f)
// 					if err != nil {
// 						if IsErrQueueClose(err) {
// 							err = q.CloseIndex()
// 							if err != nil {
// 								t.Errorf("Failed to close index: %v", err)
// 							}
// 							break
// 						}
// 						t.Errorf("dequeue failed: %v", err)
// 					}
// 				}
// 				// next confirm bulkdequeue
// 				err = q.FuncAfterBulkDequeue(tt.bulkSize, tt.lazy, f)
// 				if !IsErrQueueClose(err) {
// 					t.Errorf("Failed test: error, %v", err)
// 				}
// 			}(&wg)

// 			wg.Wait()
// 		})
// 	}
// }

// func TestQLength(t *testing.T) {
// 	type Data struct {
// 		Value int
// 	}

// 	enqueueData := []*Data{
// 		{Value: 1},
// 		{Value: 2},
// 		{Value: 3},
// 	}

// 	dir := "testdata/simple_queue/length/ffq"
// 	defer os.RemoveAll(dir)

// 	queueSize := 5
// 	maxPages := 2
// 	encoder := json.Marshal
// 	decoder := json.Unmarshal

// 	q, err := NewQueue[Data](
// 		"testQueue",
// 		WithFileDir(dir),
// 		WithQueueSize(queueSize),
// 		WithMaxPages(maxPages),
// 		WithEncoder(encoder),
// 		WithDecoder(decoder),
// 	)

// 	q.WaitInitialize()

// 	if err != nil {
// 		t.Fatalf("failed to create queue: %v", err)
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func(wg *sync.WaitGroup) {
// 		var err error
// 		defer wg.Done()
// 		err = q.BulkEnqueue(enqueueData)
// 		if err != nil {
// 			t.Errorf("enqueue failed: %v", err)
// 		}
// 		err = q.CloseQueue()
// 		if err != nil {
// 			t.Errorf("failed to close queue: %v", err)
// 		}
// 	}(&wg)
// 	wg.Wait()
// 	expected := len(enqueueData)
// 	actual := q.Length()
// 	if expected != actual {
// 		t.Errorf("Failed test: expected: %d, actual: %d", expected, actual)
// 	}
// }
