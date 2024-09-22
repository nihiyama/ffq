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

func TestNewQueue(t *testing.T) {
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
			name:        "new queue",
			fileDir:     "testdata/simple_queue/new_queue/ffq_new",
			queueSize:   5,
			maxPages:    3,
			encoder:     json.Marshal,
			decoder:     json.Unmarshal,
			afterRemove: true,
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
			actual, _ := NewQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			if tt.maxPages != actual.maxPages {
				t.Errorf("Failed test: maxPages, expect: %v, actual: %v", tt.maxPages, actual.maxPages)
			}
			if tt.fileDir != actual.fileDir {
				t.Errorf("Failed test: fileDir, expect: %v, actual: %v", tt.fileDir, actual.fileDir)
			}

			// wait initialize
			actual.WaitInitialize()

			if _, err := os.Stat(filepath.Join(tt.fileDir, indexFilename)); os.IsNotExist(err) {
				t.Errorf("index file not created")
			}
			if _, err := os.Stat(filepath.Join(tt.fileDir, "queue.0")); os.IsNotExist(err) {
				t.Errorf("queue file not created")
			}

			if err := actual.CloseQueue(); err != nil {
				t.Errorf("CloseQueue failed: %v", err)
			}
			if err := actual.CloseIndex(); err != nil {
				t.Errorf("CloseIndex failed: %v", err)
			}
		})
	}
}

func TestNewQueue_initialize_enqueue(t *testing.T) {
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_restart",
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_next_page",
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_file_rotate",
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_equal_max_page",
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_over_max_page",
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
			bq, _ := NewQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			// prepare data
			bq.WaitInitialize()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				for i := 0; i < tt.enqueNum; i++ {
					bq.Enqueue(&Data{Value: i})
				}
				bq.CloseQueue()
				wg.Done()
			}()

			for j := 0; j < tt.beforeDequeueNum; j++ {
				m, _ := bq.Dequeue()
				bq.UpdateIndex(m)
			}
			bq.CloseIndex()
			wg.Wait()
			aq, _ := NewQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)
			for j := tt.beforeDequeueNum; j < tt.enqueNum; j++ {
				m, _ := aq.Dequeue()
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
				aq.UpdateIndex(m)
			}
			aq.WaitInitialize()
			if aq.currentPage != tt.expectCurrentPage {
				t.Errorf("Failed test: page, expect: %d, actual: %d", tt.expectCurrentPage, aq.currentPage)
			}
			if aq.headGlobalIndex != tt.expectHeadGlobalIndex {
				t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", tt.expectHeadGlobalIndex, aq.headGlobalIndex)
			}
		})
	}
}

func TestNewQueue_initialize_bulk_enqueue(t *testing.T) {
	bulkSize := 3
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
			fileDir:               "testdata/simple_queue/new_queue/ffq_nitialize_bulk_restart",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              4,
			beforeDequeueNum:      (3 * bulkSize) - 2,
			expectHeadGlobalIndex: 4,
			expectCurrentPage:     0,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "next page",
			fileDir:               "testdata/simple_queue/new_queue/ffq_nitialize_bulk_next_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              5,
			beforeDequeueNum:      (4 * bulkSize) - 2,
			expectHeadGlobalIndex: 0,
			expectCurrentPage:     1,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "file rotate",
			fileDir:               "testdata/simple_queue/new_queue/ffq_initialize_bulk_file_rotate",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              7,
			beforeDequeueNum:      (6 * bulkSize) - 2,
			expectHeadGlobalIndex: 2,
			expectCurrentPage:     1,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "equal max page",
			fileDir:               "testdata/simple_queue/new_queue/ffq_nitialize_bulk_equal_max_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              15,
			beforeDequeueNum:      (14 * bulkSize) - 2,
			expectHeadGlobalIndex: 0,
			expectCurrentPage:     0,
			encoder:               json.Marshal,
			decoder:               json.Unmarshal,
			afterRemove:           true,
		},
		{
			name:                  "over max page",
			fileDir:               "testdata/simple_queue/new_queue/ffq_nitialize_bulk_over_max_page",
			queueSize:             5,
			maxPages:              3,
			enqueNum:              16,
			beforeDequeueNum:      (15 * bulkSize) - 2,
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
			bq, _ := NewQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			// prepare data
			bq.WaitInitialize()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				v := 0
				for i := 0; i < tt.enqueNum; i++ {
					data := make([]*Data, 0, bulkSize)
					for k := 0; k < bulkSize; k++ {
						data = append(data, &Data{Value: v})
						v++
					}
					bq.BulkEnqueue(data)
				}
				bq.CloseQueue()
				wg.Done()
			}()

			for j := 0; j < tt.beforeDequeueNum; j++ {
				m, _ := bq.Dequeue()
				bq.UpdateIndex(m)
			}
			bq.CloseIndex()
			wg.Wait()
			aq, _ := NewQueue[Data]("testQueue",
				WithFileDir(tt.fileDir),
				WithQueueSize(tt.queueSize),
				WithMaxPages(tt.maxPages),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)
			for j := tt.beforeDequeueNum; j < tt.enqueNum*bulkSize; j++ {
				m, _ := aq.Dequeue()
				if m.Data().Value != j {
					t.Errorf("Failed test: data, expect: %d, actual: %d", j, m.Data().Value)
				}
				gi, li := m.Index()
				if gi != j/bulkSize%tt.queueSize {
					t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", j/bulkSize%tt.queueSize, gi)
				}
				if li != j%bulkSize {
					t.Errorf("Failed test: localIndex, expect: %d, actual: %d", j%bulkSize, li)
				}
				aq.UpdateIndex(m)
			}
			aq.WaitInitialize()
			if aq.currentPage != tt.expectCurrentPage {
				t.Errorf("Failed test: page, expect: %d, actual: %d", tt.expectCurrentPage, aq.currentPage)
			}
			if aq.headGlobalIndex != tt.expectHeadGlobalIndex {
				t.Errorf("Failed test: globalIndex, expect: %d, actual: %d", tt.expectHeadGlobalIndex, aq.headGlobalIndex)
			}
		})
	}
}

func TestQEnqueueDequeue(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
	}{
		{
			name: "simple enqueue and dequeue",
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
			dir := "testdata/simple_queue/enqueue_dequeue/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			q.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, data := range tt.enqueueData {
					err := q.Enqueue(data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				err = q.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					message, err := q.Dequeue()
					if IsErrQueueClose(err) {
						break
					}
					if err != nil {
						t.Errorf("dequeue failed: %v", err)
					} else if tt.expectedDequeue[i].Value != message.Data().Value {
						t.Errorf("expected %v, actual %v", tt.expectedDequeue[i].Value, message.Data().Value)
					}
					message.Index()
					q.UpdateIndex(message)
					i++
				}
				err = q.CloseIndex()
				if err != nil {
					t.Errorf("Failed to close index: %v", err)
				}
			}(&wg)

			wg.Wait()
		})
	}
}

func TestQEnqueueDequeueWithFunc(t *testing.T) {
	type Data struct {
		Value int
	}
	tests := []struct {
		name            string
		enqueueData     []*Data
		expectedDequeue []*Data
	}{
		{
			name: "simple enqueue and dequeue",
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
	f := func(d *Data) error {
		return nil
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/simple_queue/enqueue_dequeue_with_func/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			q.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for _, data := range tt.enqueueData {
					err := q.Enqueue(data)
					if err != nil {
						t.Errorf("enqueue failed: %v", err)
					}
				}
				err = q.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					err := q.FuncAfterDequeue(f)
					if IsErrQueueClose(err) {
						break
					}
					if err != nil {
						t.Errorf("dequeue failed: %v", err)
					}
					i++
				}
				err = q.CloseIndex()
				if err != nil {
					t.Errorf("Failed to close index: %v", err)
				}
			}(&wg)

			wg.Wait()
		})
	}
}

func TestQBulkEnqueueDequeue(t *testing.T) {
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
			name: "bulk enqueue and bulk dequeue",
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
			dir := "testdata/simple_queue/bulk_enqueue_dequeue/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			q.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := q.BulkEnqueue(tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = q.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				i := 0
				for {
					messages, err := q.BulkDequeue(tt.bulkSize, tt.lazy)
					fmt.Println(err)
					if err != nil {
						if IsErrQueueClose(err) {
							err = q.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						}
						t.Errorf("dequeue failed: %v", err)
					} else if len(messages) > 0 {
						for j := i; j < len(messages); j++ {
							if messages[j].Data().Value != tt.expectedDequeue[j].Value {
								t.Errorf("expected %v, actual %v", tt.expectedDequeue[j].Value, messages[j].Data().Value)
							}
							q.UpdateIndex(messages[j])
							i++
						}
					}
				}
				// next confirm bulkdequeue
				_, err := q.BulkDequeue(tt.bulkSize, tt.lazy)
				if !IsErrQueueClose(err) {
					t.Errorf("Failed test: error, %v", err)
				}
			}(&wg)

			wg.Wait()
		})
	}
}

func TestQBulkEnqueueDequeueWithFunc(t *testing.T) {
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
			name: "bulk enqueue and bulk dequeue",
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
	f := func(d []*Data) error {
		return nil
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := "testdata/simple_queue/bulk_enqueue_dequeue/ffq"
			defer os.RemoveAll(dir)

			queueSize := 5
			maxPages := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithMaxPages(maxPages),
				WithEncoder(encoder),
				WithDecoder(decoder),
			)

			q.WaitInitialize()

			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := q.BulkEnqueue(tt.enqueueData)
				if err != nil {
					t.Errorf("enqueue failed: %v", err)
				}
				err = q.CloseQueue()
				if err != nil {
					t.Errorf("failed to close queue: %v", err)
				}
			}(&wg)

			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				for {
					err := q.FuncAfterBulkDequeue(tt.bulkSize, tt.lazy, f)
					if err != nil {
						if IsErrQueueClose(err) {
							err = q.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							break
						}
						t.Errorf("dequeue failed: %v", err)
					}
				}
				// next confirm bulkdequeue
				err := q.FuncAfterBulkDequeue(tt.bulkSize, tt.lazy, f)
				if !IsErrQueueClose(err) {
					t.Errorf("Failed test: error, %v", err)
				}
			}(&wg)

			wg.Wait()
		})
	}
}

func TestQLength(t *testing.T) {
	type Data struct {
		Value int
	}

	enqueueData := []*Data{
		{Value: 1},
		{Value: 2},
		{Value: 3},
	}

	dir := "testdata/simple_queue/length/ffq"
	defer os.RemoveAll(dir)

	queueSize := 5
	maxPages := 2
	encoder := json.Marshal
	decoder := json.Unmarshal

	q, err := NewQueue[Data](
		"testQueue",
		WithFileDir(dir),
		WithQueueSize(queueSize),
		WithMaxPages(maxPages),
		WithEncoder(encoder),
		WithDecoder(decoder),
	)

	q.WaitInitialize()

	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := q.BulkEnqueue(enqueueData)
		if err != nil {
			t.Errorf("enqueue failed: %v", err)
		}
		err = q.CloseQueue()
		if err != nil {
			t.Errorf("failed to close queue: %v", err)
		}
	}(&wg)
	wg.Wait()
	expected := len(enqueueData)
	actual := q.Length()
	if expected != actual {
		t.Errorf("Failed test: expected: %d, actual: %d", expected, actual)
	}
}
