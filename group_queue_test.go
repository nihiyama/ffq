package ffq

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewGroupQueue(t *testing.T) {
	tests := []struct {
		name        string
		fileDir     string
		queueSize   int
		maxPages    int
		encoder     func(v any) ([]byte, error)
		decoder     func(data []byte, v any) error
		afterRemove bool
	}{
		{
			name:        "exist queue",
			fileDir:     "testdata/group_queue/new_queue/ffq",
			queueSize:   5,
			maxPages:    3,
			encoder:     json.Marshal,
			decoder:     json.Unmarshal,
			afterRemove: false,
		},
		{
			name:        "new queue",
			fileDir:     "testdata/group_queue/new_queue/ffq_new",
			queueSize:   5,
			maxPages:    3,
			encoder:     json.Marshal,
			decoder:     json.Unmarshal,
			afterRemove: true,
		},
	}

	type Data struct{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.afterRemove {
				defer os.RemoveAll(tt.fileDir)
			}
			actual, _ := NewGroupQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			if tt.queueSize != actual.queueSize {
				t.Errorf("Failed test: queueSize, expect: %v, actual: %v", tt.queueSize, actual.queueSize)
			}
			if tt.maxPages != actual.maxPages {
				t.Errorf("Failed test: maxPages, expect: %v, actual: %v", tt.maxPages, actual.maxPages)
			}
			if tt.fileDir != actual.fileDir {
				t.Errorf("Failed test: fileDir, expect: %v, actual: %v", tt.fileDir, actual.fileDir)
			}

			// wait initialize
			actual.WaitInitialize()

			if err := actual.CloseQueue(); err != nil {
				t.Errorf("CloseQueue failed: %v", err)
			}
			if err := actual.CloseIndex(); err != nil {
				t.Errorf("CloseIndex failed: %v", err)
			}
		})
	}
}

func TestNewGroupQueue_initialize(t *testing.T) {
	tests := []struct {
		name                  string
		fileDir               string
		queueSize             int
		maxPages              int
		enqueNum              int
		beforeDequeueNum      int
		expectHeadGlobalIndex int
		expectCurrentPage     int
		encoder               func(v any) ([]byte, error)
		decoder               func(data []byte, v any) error
		afterRemove           bool
	}{
		{
			name:                  "restart",
			fileDir:               "testdata/group_queue/new_queue/ffq_initialize_restart",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              4,
			beforeDequeueNum:      3,
			expectHeadGlobalIndex: 4,
			expectCurrentPage:     0,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "next page",
			fileDir:               "testdata/group_queue/new_queue/ffq_initialize_next_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              5,
			beforeDequeueNum:      3,
			expectHeadGlobalIndex: 0,
			expectCurrentPage:     1,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "file rotate",
			fileDir:               "testdata/group_queue/new_queue/ffq_initialize_file_rotate",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              7,
			beforeDequeueNum:      3,
			expectHeadGlobalIndex: 2,
			expectCurrentPage:     1,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "equal max page",
			fileDir:               "testdata/group_queue/new_queue/ffq_initialize_equal_max_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              15,
			beforeDequeueNum:      13,
			expectHeadGlobalIndex: 0,
			expectCurrentPage:     0,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "over max page",
			fileDir:               "testdata/group_queue/new_queue/ffq_initialize_over_max_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              16,
			beforeDequeueNum:      13,
			expectHeadGlobalIndex: 1,
			expectCurrentPage:     0,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
	}

	type Data struct {
		Value int
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.afterRemove {
				defer os.RemoveAll(tt.fileDir)
			}
			bgq, _ := NewGroupQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			// prepare data
			bgq.WaitInitialize()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < tt.enqueNum; i++ {
					bgq.Enqueue("test", &Data{Value: i})
				}
				bgq.CloseQueue()
				wg.Done()
			}()

			j := 0
			for {
				if j >= tt.beforeDequeueNum {
					break
				}
				ms, _ := bgq.Dequeue()
				for m := range ms {
					bgq.UpdateIndex(m)
					j++
				}
			}
			bgq.CloseIndex()
			wg.Wait()
			agq, _ := NewGroupQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			for {
				if j == tt.enqueNum {
					break
				}
				ms, _ := agq.Dequeue()
				for m := range ms {
					if m.Data().Value != j {
						t.Errorf("Failed test: data, expect: %d, actual: %d", j, m.Data().Value)
					}
					gi, li := m.Index()
					if gi != j%tt.queueSize {
						t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", j%tt.queueSize, gi)
					}
					if li != 0 {
						t.Errorf("Failed test: localIndex, expect: 0, actual: %d", li)
					}
					agq.UpdateIndex(m)
					j++
				}
			}
			agq.WaitInitialize()
			if agq.queues["test"].currentPage != tt.expectCurrentPage {
				t.Errorf("Failed test: page, expect: %d, actual: %d", tt.expectCurrentPage, agq.queues["test"].currentPage)
			}
			if agq.queues["test"].headGlobalIndex != tt.expectHeadGlobalIndex {
				t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", tt.expectHeadGlobalIndex, agq.queues["test"].headGlobalIndex)
			}
		})
	}
}

func TestGQEnqueueDequeue(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
	}{
		{
			name: "group enqueue and dequeue",
			enqueueData: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			expectedDequeue: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/group_queue/enqueue_dequeue/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			gq, err := NewGroupQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			gq.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue1", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue2", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue3", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				err = gq.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					messages, err := gq.Dequeue()
					if err != nil {
						if IsErrQueueClose(err) {
							err = gq.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						} else {
							t.Errorf("dequeue failed: %v", err)
						}
					}
					for m := range messages {
						m.Data()
						m.Index()
						m.Name()
						gq.UpdateIndex(m)
						i++
					}
				}
			}(&wg)
			wg.Wait()
		})
	}
}

func TestGQEnqueueDequeueWithFunc(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
	}{
		{
			name: "group enqueue and dequeue",
			enqueueData: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			expectedDequeue: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/group_queue/enqueue_dequeue_with_func/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			gq, err := NewGroupQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			gq.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue1", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue2", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				for _, data := range tt.enqueueData {
					err := gq.Enqueue("queue3", data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				err = gq.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			totalDataNum := len(tt.enqueueData) * 3
			f := func(d *Data) error {
				totalDataNum--
				return nil
			}

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					err := gq.FuncAfterDequeue(f)
					if err != nil {
						if IsErrQueueClose(err) {
							err = gq.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						} else {
							t.Errorf("dequeue failed: %v", err)
						}
					}
				}
			}(&wg)
			wg.Wait()
		})
	}
}

func TestGQBulkEnqueueDequeue(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
		bulkSize        int
		lazy            time.Duration
	}{
		{
			name: "bulk group enqueue and dequeue",
			enqueueData: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			expectedDequeue: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			bulkSize: 4,
			lazy:     10 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/group_queue/bulk_enqueue_dequeue/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			gq, err := NewGroupQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			gq.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := gq.BulkEnqueue("queue1", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = gq.BulkEnqueue("queue2", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = gq.BulkEnqueue("queue3", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}

				err = gq.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					messages, err := gq.BulkDequeue(tt.bulkSize, tt.lazy)
					if err != nil {
						if IsErrQueueClose(err) {
							err = gq.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						} else {
							t.Errorf("dequeue failed: %v", err)
						}
					}
					for ms := range messages {
						for _, m := range ms {
							m.Data()
							m.Index()
							m.Name()
							gq.UpdateIndex(m)
							i++
						}
					}
				}
			}(&wg)
			wg.Wait()
		})
	}
}

func TestGQBulkEnqueueDequeueWithFunc(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
		bulkSize        int
		lazy            time.Duration
	}{
		{
			name: "bulk group enqueue and dequeue",
			enqueueData: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			expectedDequeue: []*Data{
				{Value: 1},
				{Value: 2},
				{Value: 3},
				{Value: 4},
				{Value: 5},
				{Value: 6},
				{Value: 7},
				{Value: 8},
				{Value: 9},
				{Value: 10},
				{Value: 11},
				{Value: 12},
				{Value: 13},
				{Value: 14},
				{Value: 15},
			},
			bulkSize: 4,
			lazy:     10 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/group_queue/bulk_enqueue_dequeue_with_func/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			gq, err := NewGroupQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			gq.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := gq.BulkEnqueue("queue1", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = gq.BulkEnqueue("queue2", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = gq.BulkEnqueue("queue3", tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}

				err = gq.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			totalDataNum := len(tt.enqueueData) * 3
			f := func(d []*Data) error {
				totalDataNum -= len(d)
				return nil
			}

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					err := gq.FuncAfterBulkDequeue(tt.bulkSize, tt.lazy, f)
					if err != nil {
						if IsErrQueueClose(err) {
							err = gq.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						} else {
							t.Errorf("dequeue failed: %v", err)
						}
					}
				}
			}(&wg)
			wg.Wait()
		})
	}
}

func TestGQLength(t *testing.T) {
	type Data struct {
		Value int
	}

	enqueueData := []*Data{
		{Value: 1},
		{Value: 2},
		{Value: 3},
	}

	dir := "testdata/group_queue/length/ffq"
	defer os.RemoveAll(dir)

	queueSize := 5
	maxPages := 2
	encoder := json.Marshal
	decoder := json.Unmarshal

	gq, err := NewGroupQueue[Data](
		"testQueue",
		WithFileDir(dir),
		WithQueueSize(queueSize),
		WithMaxPages(maxPages),
		WithEncoder(encoder),
		WithDecoder(decoder),
	)

	gq.WaitInitialize()

	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	testQueues := []string{"queue1", "queue2", "queue3"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for _, tq := range testQueues {
			err := gq.BulkEnqueue(tq, enqueueData)
			if err != nil {
				t.Errorf("enqueue failed: %v", err)
			}
		}
		err = gq.CloseQueue()
		if err != nil {
			t.Errorf("failed to close queue: %v", err)
		}
	}(&wg)
	wg.Wait()
	expected := len(enqueueData)
	actual := gq.Length()
	for _, l := range actual {
		if expected != l {
			t.Errorf("Failed test: expected: %d, actual: %d", expected, l)
		}
	}

}
