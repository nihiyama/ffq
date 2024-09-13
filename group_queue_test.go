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
		name             string
		fileDir          string
		queueSize        int
		enqueueWriteSize int
		maxPages         int
		dataFixedLength  uint64
		encoder          func(v any) ([]byte, error)
		decoder          func(data []byte, v any) error
		afterRemove      bool
	}{
		{
			name:             "exist queue",
			fileDir:          "testdata/group_queue/new_queue/ffq",
			queueSize:        5,
			enqueueWriteSize: 10,
			maxPages:         3,
			dataFixedLength:  4,
			encoder:          json.Marshal,
			decoder:          json.Unmarshal,
			afterRemove:      false,
		},
		{
			name:             "new queue",
			fileDir:          "testdata/group_queue/new_queue/ffq_new",
			queueSize:        5,
			enqueueWriteSize: 10,
			maxPages:         3,
			dataFixedLength:  4,
			encoder:          json.Marshal,
			decoder:          json.Unmarshal,
			afterRemove:      true,
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
