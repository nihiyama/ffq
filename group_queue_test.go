package ffq

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Group queue operations.

func TestGroupWaitInitializeWithErrorFailures(t *testing.T) {
	t.Run("one-good-one-bad-child", func(t *testing.T) {
		dir := t.TempDir()
		for name, data := range map[string]string{"good": "[1]\n", "bad": "["} {
			child := filepath.Join(dir, name)
			if err := os.Mkdir(child, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(child, "queue.0"), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
		g, err := NewGroupQueue[int]("g", WithFileDir(dir), WithQueueSize(2))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { g.CloseQueue(); g.CloseIndex() }()
		if err := g.WaitInitializeWithError(); !isRecoveryFailure(err) {
			t.Fatalf("failed child accepted: %v", err)
		}
	})
	t.Run("directory-list", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		g := &GroupQueue[int]{fileDir: path, groupSize: 2, initializeBlock: make(chan struct{}), failureSig: make(chan struct{})}
		go g.initialize()
		if err := g.WaitInitializeWithError(); err == nil || IsErrQueueClose(err) {
			t.Fatalf("directory failure: %v", err)
		}
		g.WaitInitialize()
	})
	t.Run("too-many-children", func(t *testing.T) {
		dir := t.TempDir()
		for _, name := range []string{"a", "b", "c"} {
			if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
				t.Fatal(err)
			}
		}
		g, err := NewGroupQueue[int]("g", WithFileDir(dir), WithGroupSize(2))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { g.CloseQueue(); g.CloseIndex() }()
		if err := g.WaitInitializeWithError(); err == nil || IsErrQueueClose(err) {
			t.Fatalf("capacity failure: %v", err)
		}
	})
}

func TestGroupClosedEnqueueErrors(t *testing.T) {
	for _, kind := range []QueueType{SPSC, MPSC} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing=%t", kind, existing), func(t *testing.T) {
				dir := t.TempDir()
				g, err := NewGroupQueue[int]("closed", WithFileDir(dir), WithQueueSize(4), WithQueueType(kind))
				if err != nil {
					t.Fatal(err)
				}
				g.WaitInitialize()
				defer g.CloseIndex()
				value := 1
				if existing {
					if err := g.Enqueue("q", &value); err != nil {
						t.Fatal(err)
					}
					m, err := g.Dequeue()
					if err != nil {
						t.Fatal(err)
					}
					if err := g.UpdateIndex(m); err != nil {
						t.Fatal(err)
					}
				}
				if err := g.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"q", "new"} {
					for _, bulk := range []bool{false, true} {
						t.Run(fmt.Sprintf("name=%s/bulk=%t", name, bulk), func(t *testing.T) {
							var err error
							if bulk {
								err = g.BulkEnqueue(name, []*int{&value})
							} else {
								err = g.Enqueue(name, &value)
							}
							if !IsErrQueueClose(err) {
								t.Fatalf("closed enqueue is not recognizable: %v", err)
							}
						})
					}
				}
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				want := 0
				if existing {
					want = 1
				}
				if len(entries) != want {
					t.Fatal("closed enqueue created another child directory")
				}
			})
		}
	}
}

func TestBoundaryGroupBulkDistribution(t *testing.T) {
	for _, children := range []int{0, 1, 2, 3, 4} {
		for _, callback := range []bool{false, true} {
			for _, limit := range []int{0, 1, 2, 3, 4, 5, 13} {
				t.Run(fmt.Sprintf("children=%d/callback=%t/limit=%d", children, callback, limit), func(t *testing.T) {
					capacity := children
					if capacity == 0 {
						capacity = 1
					}
					g, err := NewGroupQueue[int]("g", WithFileDir(t.TempDir()), WithQueueSize(4), WithMaxPage(2), WithGroupSize(capacity))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { g.CloseQueue(); g.CloseIndex() })
					g.WaitInitialize()
					for child := 0; child < children; child++ {
						if err := g.BulkEnqueue(fmt.Sprint(child), boundaryItems(child*10, 3)); err != nil {
							t.Fatal(err)
						}
					}
					if err := g.CloseQueue(); err != nil {
						t.Fatal(err)
					}
					seen := make([]int, children)
					check := func(value int) {
						child, sequence := value/10, value%10
						if child < 0 || child >= children || sequence != seen[child] {
							t.Fatalf("out of order/duplicate item %d; seen=%v", value, seen)
						}
						seen[child]++
					}
					count := 0
					if callback {
						var calledCount int
						count, err = g.FuncAfterBulkDequeue(uint64(limit), time.Second, func(items []*int) error {
							calledCount += len(items)
							for _, item := range items {
								check(*item)
							}
							return nil
						})
						if calledCount != count {
							t.Fatalf("count=%d, callback received=%d", count, calledCount)
						}
					} else {
						var messages []*Message[int]
						messages, err = g.BulkDequeue(uint64(limit), time.Second)
						count = len(messages)
						for _, m := range messages {
							check(*m.Item())
							if m.Name() != fmt.Sprint(*m.Item()/10) {
								t.Fatalf("message routed to wrong child: %q", m.Name())
							}
							if err := g.UpdateIndex(m); err != nil {
								t.Fatal(err)
							}
						}
					}
					if children == 0 {
						validError := IsErrQueueClose(err)
						if limit == 0 {
							validError = IsErrQueueOption(err)
						}
						if count != 0 || !validError {
							t.Fatalf("closed empty group: count=%d, error=%v", count, err)
						}
						return
					}
					want := children * 3
					if want > limit {
						want = limit
					}
					validError := err == nil
					if limit == 0 {
						validError = IsErrQueueOption(err)
					}
					if !validError || count != want {
						t.Fatalf("first batch: count=%d, error=%v, want=%d", count, err, want)
					}
					for child, received := range seen {
						q, err := g.getQueue(fmt.Sprint(child))
						if err != nil {
							t.Fatal(err)
						}
						data, err := os.ReadFile(filepath.Join(q.fileDir, "index"))
						if err != nil {
							t.Fatal(err)
						}
						if received == 0 {
							if len(data) != 0 {
								t.Fatalf("untouched child %d was acknowledged", child)
							}
						} else if len(data) != 8 || binary.LittleEndian.Uint64(data) != uint64(received-1) {
							t.Fatalf("child %d has wrong progress: %x, received=%d", child, data, received)
						}
					}
					for {
						m, err := g.Dequeue()
						if IsErrQueueClose(err) {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						check(*m.Item())
						if err := g.UpdateIndex(m); err != nil {
							t.Fatal(err)
						}
					}
					for child, received := range seen {
						if received != 3 {
							t.Errorf("child %d lost items: received=%d", child, received)
						}
					}
					if err := g.CloseIndex(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestBoundaryGroupCapacity(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			g, err := NewGroupQueue[int]("g", WithFileDir(t.TempDir()), WithGroupSize(limit), WithQueueSize(2))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { g.CloseQueue(); g.CloseIndex() })
			g.WaitInitialize()
			for i := 0; i < limit; i++ {
				value := i * 10
				if err := g.Enqueue(fmt.Sprint(i), &value); err != nil {
					t.Fatal(err)
				}
			}
			value := 999
			if err := g.Enqueue("overflow", &value); err == nil {
				t.Fatal("accepted a child beyond the configured limit")
			}
			if _, err := os.Stat(filepath.Join(g.fileDir, "overflow")); !os.IsNotExist(err) {
				t.Fatalf("rejected child created a directory: %v", err)
			}
			if int(atomic.LoadUint32(&g.queueCount)) != limit {
				t.Fatal("rejected child changed queue count")
			}
			value = 1
			if err := g.Enqueue("0", &value); err != nil {
				t.Fatalf("existing child rejected at group limit: %v", err)
			}
			if err := g.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			seen := make([]int, limit)
			for {
				m, err := g.Dequeue()
				if IsErrQueueClose(err) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				child, sequence := *m.Item()/10, *m.Item()%10
				if child >= limit || child < 0 || sequence != seen[child] || m.Name() != fmt.Sprint(child) {
					t.Fatalf("unexpected message %d/%q, seen=%v", *m.Item(), m.Name(), seen)
				}
				seen[child]++
				if err := g.UpdateIndex(m); err != nil {
					t.Fatal(err)
				}
			}
			for child, count := range seen {
				want := 1
				if child == 0 {
					want = 2
				}
				if count != want {
					t.Errorf("child %d: count=%d, want %d", child, count, want)
				}
			}
			if err := g.CloseIndex(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGroupProgressRejectsInvalidMessages(t *testing.T) {
	group, err := NewGroupQueue[int]("g", WithFileDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewGroupQueue[int]("other", WithFileDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { group.CloseQueue(); group.CloseIndex(); other.CloseQueue(); other.CloseIndex() })
	group.WaitInitialize()
	other.WaitInitialize()
	value := 1
	if err := group.Enqueue("a", &value); err != nil {
		t.Fatal(err)
	}
	own, err := group.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Enqueue("a", &value); err != nil {
		t.Fatal(err)
	}
	foreign, err := other.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Enqueue("unknown", &value); err != nil {
		t.Fatal(err)
	}
	unknown, err := other.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	q, err := group.getQueue("a")
	if err != nil {
		t.Fatal(err)
	}
	before := diskSnapshot(t, q.fileDir)
	for _, tc := range []struct {
		name    string
		message *Message[int]
		missing bool
	}{
		{name: "nil"},
		{name: "foreign instance with same name", message: foreign},
		{name: "unknown queue name", message: unknown, missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := group.UpdateIndex(tc.message)
			if err == nil || tc.missing && !IsErrQueueNotFound(err) || !tc.missing && !isProgressFailure(err) {
				t.Fatalf("invalid progress result: %v", err)
			}
			if q.ackNext.Load() != 0 || !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
				t.Fatal("invalid message changed data or progress")
			}
		})
	}
	if err := group.UpdateIndex(own); err != nil {
		t.Fatalf("valid progress update: %v", err)
	}
}

func TestGroupBulkPublicationWithoutPerItemNotifications(t *testing.T) {
	for _, bulkConsumer := range []bool{false, true} {
		for _, producers := range []int{1, 2} {
			t.Run(fmt.Sprintf("bulk=%t/producers=%d", bulkConsumer, producers), func(t *testing.T) {
				kind := SPSC
				if producers > 1 {
					kind = MPSC
				}
				gq, err := NewGroupQueue[int]("g", WithFileDir(t.TempDir()), WithQueueSize(2), WithMaxPage(3), WithQueueType(kind))
				if err != nil {
					t.Fatal(err)
				}
				seen := make([]int, producers)
				done := make(chan error, 1)
				go func() {
					for {
						var batch []*Message[int]
						var err error
						if bulkConsumer {
							batch, err = gq.BulkDequeue(3, time.Millisecond)
						} else {
							var message *Message[int]
							message, err = gq.Dequeue()
							if message != nil {
								batch = []*Message[int]{message}
							}
						}
						if IsErrQueueClose(err) {
							done <- gq.CloseIndex()
							return
						}
						if err != nil {
							done <- err
							return
						}
						for _, message := range batch {
							if message.Name() != "q" {
								done <- fmt.Errorf("unexpected queue name: %q", message.Name())
								return
							}
							value := *message.Item()
							producer := value / 100
							if producer >= len(seen) || value%100 != seen[producer] {
								done <- fmt.Errorf("unexpected order: %d", value)
								return
							}
							seen[producer]++
							if err := gq.UpdateIndex(message); err != nil {
								done <- err
								return
							}
						}
					}
				}()
				gq.WaitInitialize()
				finished := make(chan error, producers)
				for producer := 0; producer < producers; producer++ {
					go func(producer int) {
						items := make([]*int, 7)
						for i := range items {
							value := producer*100 + i
							items[i] = &value
						}
						finished <- gq.BulkEnqueue("q", items)
					}(producer)
				}
				for producer := 0; producer < producers; producer++ {
					select {
					case err := <-finished:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						gq.CloseQueue()
						t.Fatal("bulk producer blocked")
					}
				}
				if err := gq.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("consumer did not stop")
				}
				for producer, count := range seen {
					if count != 7 {
						t.Fatalf("producer %d: consumed %d", producer, count)
					}
				}
			})
		}
	}
}

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
		wantTail    uint64
	}{
		{
			name:        "simple enqueue",
			enqueueNums: 8,
			bulkSize:    5,
			wantTail:    8,
		},
		{
			name:        "with file rotate",
			enqueueNums: 15,
			bulkSize:    7,
			wantTail:    15,
		},
		{
			name:        "tail reset 0",
			enqueueNums: 25,
			bulkSize:    12,
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
				for {
					m, err := gq.Dequeue()
					if IsErrQueueClose(err) {
						if err := gq.CloseIndex(); err != nil {
							t.Errorf("close index: %v", err)
						}
						return
					}
					if err != nil {
						t.Errorf("dequeue: %v", err)
						return
					}
					if err := gq.UpdateIndex(m); err != nil {
						t.Errorf("update index: %v", err)
					}
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
		{
			name:        "size less than child count",
			enqueueNums: 8,
			size:        1,
			lazy:        time.Millisecond,
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
			seen := make(map[string]int, len(queueNames))
			for _, name := range queueNames {
				seen[name] = 0
			}
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
						return
					}
					if uint64(len(ms)) > tt.size {
						t.Errorf("bulk limit exceeded: got %d, limit %d", len(ms), tt.size)
						return
					}
					for _, m := range ms {
						if m == nil || m.Item() == nil {
							t.Error("nil message or item")
							return
						}
						next, ok := seen[m.Name()]
						if !ok || m.Item().Value != next {
							t.Errorf("queue %q: got value %d, want %d", m.Name(), m.Item().Value, next)
							return
						}
						seen[m.Name()]++
						if err := gq.UpdateIndex(m); err != nil {
							t.Errorf("update index: %v", err)
							return
						}
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
			for name, count := range seen {
				if count != tt.enqueueNums {
					t.Errorf("queue %q: got %d messages, want %d", name, count, tt.enqueueNums)
				}
			}
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
						if err == nil {
							t.Error("callback error was not returned")
						}
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
						if err == nil {
							t.Error("callback error was not returned")
						}
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
	if !IsErrQueueClose(err) {
		t.Errorf("want queue close error, got %v", err)
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

// Recovery and child queue coordination.

func TestGroupMultipleChildrenRecoveryBeyondBuffer(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		q, err := NewQueue[int](name, WithFileDir(filepath.Join(dir, name)), WithQueueSize(2), WithMaxPage(3))
		if err != nil {
			t.Fatal(err)
		}
		q.WaitInitialize()
		v := 0
		if err := q.Enqueue(&v); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Dequeue(); err != nil {
			t.Fatal(err)
		}
		for _, value := range []int{1, 2} {
			v := value
			if err := q.Enqueue(&v); err != nil {
				t.Fatal(err)
			}
		}
		q.CloseQueue()
		q.CloseIndex()
	}
	gq, err := NewGroupQueue[int]("g", WithFileDir(dir), WithQueueSize(2), WithMaxPage(3))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	got := make(map[string][]int)
	go func() {
		for {
			batch, err := gq.BulkDequeue(50, 10*time.Millisecond)
			if IsErrQueueClose(err) {
				done <- gq.CloseIndex()
				return
			}
			if err != nil {
				done <- err
				return
			}
			for _, m := range batch {
				got[m.Name()] = append(got[m.Name()], *m.Item())
				if err := gq.UpdateIndex(m); err != nil {
					done <- err
					return
				}
			}
		}
	}()
	initialized := make(chan error, 1)
	go func() { initialized <- gq.waitInitialize() }()
	select {
	case err := <-initialized:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		gq.CloseQueue()
		t.Fatal("multiple-child recovery blocked")
	}
	if err := gq.CloseQueue(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		if !reflect.DeepEqual(got[name], []int{0, 1, 2}) {
			t.Fatalf("%s: %v", name, got[name])
		}
	}
}

func TestGroupRecoveryBeyondBuffer(t *testing.T) {
	for _, size := range []uint64{1, 2, 2048} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			dir := t.TempDir()
			q, err := NewQueue[int]("child", WithFileDir(filepath.Join(dir, "child")), WithQueueSize(size), WithMaxPage(3))
			if err != nil {
				t.Fatal(err)
			}
			q.WaitInitialize()
			v := 0
			if err := q.Enqueue(&v); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Dequeue(); err != nil {
				t.Fatal(err)
			} // one interrupted message
			for i := 1; i <= int(size); i++ {
				v := i
				if err := q.Enqueue(&v); err != nil {
					t.Fatal(err)
				}
			}
			q.CloseQueue()
			q.CloseIndex()
			gq, err := NewGroupQueue[int]("g", WithFileDir(dir), WithQueueSize(size), WithMaxPage(3))
			if err != nil {
				t.Fatal(err)
			}
			var got []int
			done := make(chan error, 1)
			go func() {
				for {
					messages, err := gq.BulkDequeue(50, 10*time.Millisecond)
					if IsErrQueueClose(err) {
						done <- gq.CloseIndex()
						return
					}
					if err != nil {
						done <- err
						return
					}
					for _, m := range messages {
						got = append(got, *m.Item())
						if err := gq.UpdateIndex(m); err != nil {
							done <- err
							return
						}
					}
				}
			}()
			initialized := make(chan error, 1)
			go func() { initialized <- gq.waitInitialize() }()
			select {
			case err := <-initialized:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				gq.CloseQueue()
				t.Fatal("group recovery did not complete")
			}
			if err := gq.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			want := make([]int, int(size)+1)
			for i := range want {
				want[i] = i
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("restored %d of %d messages", len(got), len(want))
			}
			if err := gq.waitInitialize(); err != nil {
				t.Fatal("repeated wait:", err)
			}
		})
	}
}

func TestGroupInitializationFailureAndEmptyClose(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			dir := t.TempDir()
			if invalid {
				if err := os.Mkdir(filepath.Join(dir, "bad"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "bad", "queue.0"), []byte("["), 0600); err != nil {
					t.Fatal(err)
				}
			}
			gq, err := NewGroupQueue[int]("g", WithFileDir(dir))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := gq.Dequeue(); done <- err }()
			err = gq.waitInitialize()
			if invalid && !isRecoveryFailure(err) || !invalid && err != nil {
				t.Fatalf("initialization: %v", err)
			}
			if err := gq.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if invalid && !isRecoveryFailure(err) || !invalid && !IsErrQueueClose(err) {
					t.Fatalf("dequeue: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("group consumer was not released")
			}
			if err := gq.CloseIndex(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGroupConcurrentCreationOfSameChild(t *testing.T) {
	gq, err := NewGroupQueue[int]("g", WithFileDir(t.TempDir()), WithQueueSize(8), WithQueueType(MPSC))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		count := 0
		seen := make([]bool, 80)
		next := make([]int, 8)
		for {
			m, err := gq.Dequeue()
			if IsErrQueueClose(err) {
				if count != 80 {
					done <- fmt.Errorf("got %d messages", count)
					return
				}
				done <- gq.CloseIndex()
				return
			}
			if err != nil {
				done <- err
				return
			}
			value := *m.Item()
			if value < 0 || value >= len(seen) || seen[value] {
				done <- fmt.Errorf("duplicate or invalid value %d", value)
				return
			}
			producer := value / 10
			if value%10 != next[producer] {
				done <- fmt.Errorf("producer %d: out of order value %d", producer, value)
				return
			}
			seen[value] = true
			next[producer]++
			if err := gq.UpdateIndex(m); err != nil {
				done <- err
				return
			}
			count++
		}
	}()
	if err := gq.waitInitialize(); err != nil {
		t.Fatal(err)
	}
	var producers sync.WaitGroup
	for producer := 0; producer < 8; producer++ {
		producers.Add(1)
		go func(producer int) {
			defer producers.Done()
			for i := 0; i < 10; i++ {
				v := producer*10 + i
				if err := gq.Enqueue("child", &v); err != nil {
					t.Error(err)
					return
				}
			}
		}(producer)
	}
	producers.Wait()
	if got := atomic.LoadUint32(&gq.queueCount); got != 1 {
		t.Fatalf("created %d children", got)
	}
	if err := gq.CloseQueue(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGroupCapacityDoesNotRemoveExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "queue.0"), []byte("[1]\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gq, err := NewGroupQueue[int]("g", WithFileDir(dir), WithGroupSize(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := gq.waitInitialize(); err == nil {
		t.Fatal("excess children accepted")
	}
	gq.CloseQueue()
	gq.CloseIndex()
	for _, name := range []string{"a", "b"} {
		got, err := os.ReadFile(filepath.Join(dir, name, "queue.0"))
		if err != nil || string(got) != "[1]\n" {
			t.Fatalf("changed existing child %s", name)
		}
	}
}

func TestGroupFailureCancelsOtherRecovery(t *testing.T) {
	dir := t.TempDir()
	q, err := NewQueue[int]("a", WithFileDir(filepath.Join(dir, "a")), WithQueueSize(2), WithMaxPage(3))
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	v := 0
	if err := q.Enqueue(&v); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Dequeue(); err != nil {
		t.Fatal(err)
	}
	a, b := 1, 2
	if err := q.BulkEnqueue([]*int{&a, &b}); err != nil {
		t.Fatal(err)
	}
	q.CloseQueue()
	q.CloseIndex()
	if err := os.Mkdir(filepath.Join(dir, "z"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "z", "queue.0"), []byte("["), 0600); err != nil {
		t.Fatal(err)
	}
	gq, err := NewGroupQueue[int]("g", WithFileDir(dir), WithQueueSize(2), WithMaxPage(3))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	// No consumer is needed to discover the failing child. Otherwise the first
	// child's full replay buffer could hide the second child's failure forever.
	go func() { done <- gq.waitInitialize() }()
	select {
	case err := <-done:
		if !isRecoveryFailure(err) {
			t.Fatalf("error: %v", err)
		}
	case <-time.After(3 * time.Second):
		gq.CloseQueue()
		t.Fatal("another child hid initialization failure")
	}
	gq.CloseQueue()
	gq.CloseIndex()
	q, err = NewQueue[int]("a", WithFileDir(filepath.Join(dir, "a")), WithQueueSize(2), WithMaxPage(3))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatal(got)
	}
}

func TestGroupIOFailureReleasesOtherProducer(t *testing.T) {
	dir := t.TempDir()
	gq, err := NewGroupQueue[int]("g", WithFileDir(dir), WithQueueSize(2), WithMaxPage(3), WithQueueType(MPSC))
	if err != nil {
		t.Fatal(err)
	}
	if err := gq.waitInitialize(); err != nil {
		t.Fatal(err)
	}
	v := 0
	if err := gq.Enqueue("a", &v); err != nil {
		t.Fatal(err)
	}
	for _, value := range []int{10, 11} {
		v := value
		if err := gq.Enqueue("b", &v); err != nil {
			t.Fatal(err)
		}
	}
	producer := make(chan error, 1)
	go func() { v := 12; producer <- gq.Enqueue("b", &v) }()
	waitForQueueFunction(t, "publish")
	a, err := gq.getQueue("a")
	if err != nil {
		t.Fatal(err)
	}
	m, err := a.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.indexFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gq.UpdateIndex(m); !isIOFailure(err) {
		t.Fatalf("index failure: %v", err)
	}
	select {
	case err := <-producer:
		if !isIOFailure(err) {
			t.Fatalf("producer: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("another child's producer stayed blocked")
	}
	if err := gq.CloseQueue(); err != nil {
		t.Fatal(err)
	}
	gq.CloseIndex() // a's descriptor was deliberately closed by the fault injection
	for _, tc := range []struct {
		name string
		want []int
	}{{"a", []int{0}}, {"b", []int{10, 11, 12}}} {
		q, err := NewQueue[int](tc.name, WithFileDir(filepath.Join(dir, tc.name)), WithQueueSize(2), WithMaxPage(3))
		if err != nil {
			t.Fatal(err)
		}
		if got := collectRecovered(t, q); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: %v", tc.name, got)
		}
	}
}
