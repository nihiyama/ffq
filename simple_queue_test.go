package ffq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	tests := []struct {
		name             string
		fileDir          string
		queueSize        int
		enqueueWriteSize int
		pageSize         int
		dataFixedLength  uint64
		encoder          func(v any) ([]byte, error)
		decoder          func(data []byte, v any) error
		afterRemove      bool
	}{
		// {
		// 	name:             "exist queue",
		// 	fileDir:          "testdata/simple_queue/new_queue/ffq",
		// 	queueSize:        5,
		// 	enqueueWriteSize: 10,
		// 	pageSize:         3,
		// 	dataFixedLength:  4,
		// 	encoder:          json.Marshal,
		// 	decoder:          json.Unmarshal,
		// 	afterRemove:      false,
		// },
		{
			name:             "new queue",
			fileDir:          "testdata/simple_queue/new_queue/ffq_new",
			queueSize:        5,
			enqueueWriteSize: 10,
			pageSize:         3,
			dataFixedLength:  4,
			encoder:          json.Marshal,
			decoder:          json.Unmarshal,
			afterRemove:      true,
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
				WithPageSize(tt.pageSize),
				WithEncoder(tt.encoder),
				WithDecoder(tt.decoder),
			)

			if tt.queueSize != actual.queueSize {
				t.Errorf("Failed test: queueSize, expect: %v, actual: %v", tt.queueSize, actual.queueSize)
			}
			if tt.pageSize != actual.pageSize {
				t.Errorf("Failed test: pageSize, expect: %v, actual: %v", tt.pageSize, actual.pageSize)
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
			pageSize := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithPageSize(pageSize),
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
				for i := 0; i < len(tt.expectedDequeue); i++ {
					message, err := q.Dequeue()
					if err != nil {
						t.Errorf("dequeue failed: %v", err)
					} else if tt.expectedDequeue[i].Value != message.Data().Value {
						t.Errorf("expected %v, actual %v", tt.expectedDequeue[i].Value, message.Data().Value)
					}
					message.Index()
					q.UpdateIndex(message)
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
			pageSize := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithPageSize(pageSize),
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
				for i := 0; i < len(tt.expectedDequeue); i++ {
					err := q.FuncAfterDequeue(f)
					if err != nil {
						t.Errorf("dequeue failed: %v", err)
					}
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
			pageSize := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithPageSize(pageSize),
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
				for i < len(tt.expectedDequeue) {
					messages, err := q.BulkDequeue(tt.bulkSize, tt.lazy)
					if err != nil {
						if IsErrQueueClose(err) {
							err = q.CloseIndex()
							if err != nil {
								t.Errorf("Failed to close index: %v", err)
							}
							return
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
			pageSize := 2
			encoder := json.Marshal
			decoder := json.Unmarshal

			q, err := NewQueue[Data](
				"testQueue",
				WithFileDir(dir),
				WithQueueSize(queueSize),
				WithPageSize(pageSize),
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
							return
						}
						t.Errorf("dequeue failed: %v", err)
					}
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
	pageSize := 2
	encoder := json.Marshal
	decoder := json.Unmarshal

	q, err := NewQueue[Data](
		"testQueue",
		WithFileDir(dir),
		WithQueueSize(queueSize),
		WithPageSize(pageSize),
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
