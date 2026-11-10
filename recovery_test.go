package ffq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type initializationReceiver interface {
	boundaryReceiver
	WaitInitialize()
	WaitInitializeWithError() error
}

func newInitializationReceiver(t *testing.T, grouped bool, dir string, opts ...Option) (initializationReceiver, func([]*int) error) {
	t.Helper()
	opts = append([]Option{WithFileDir(dir)}, opts...)
	var r initializationReceiver
	var enqueue func([]*int) error
	if grouped {
		g, err := NewGroupQueue[int]("g", opts...)
		if err != nil {
			t.Fatal(err)
		}
		r, enqueue = g, func(items []*int) error { return g.BulkEnqueue("q", items) }
	} else {
		q, err := NewQueue[int]("q", opts...)
		if err != nil {
			t.Fatal(err)
		}
		r, enqueue = q, q.BulkEnqueue
	}
	t.Cleanup(func() { r.CloseQueue(); r.CloseIndex() })
	return r, enqueue
}

func initializationResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("initialization operation did not finish")
		return nil
	}
}

func initializationChild(t *testing.T, r initializationReceiver) *Queue[int] {
	t.Helper()
	if q, ok := r.(*Queue[int]); ok {
		return q
	}
	g := r.(*GroupQueue[int])
	// A decoder can run before registerQueue finishes publishing the child.
	g.addMu.Lock()
	defer g.addMu.Unlock()
	q, err := g.getQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestWaitInitializeWithErrorRecovery(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, count := range []int{0, 1, 2049} {
				t.Run(fmt.Sprintf("group=%t/%s/pending=%d", grouped, kind, count), func(t *testing.T) {
					dir := t.TempDir()
					dataDir := dir
					if grouped {
						dataDir = filepath.Join(dir, "q")
					}
					if count > 0 {
						if err := os.MkdirAll(dataDir, 0700); err != nil {
							t.Fatal(err)
						}
						for start := 0; start < count; start += 2048 {
							n := count - start
							if n > 2048 {
								n = 2048
							}
							data, err := json.Marshal(boundaryItems(start, n))
							if err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(filepath.Join(dataDir, fmt.Sprintf("queue.%d", start/2048)), append(data, '\n'), 0600); err != nil {
								t.Fatal(err)
							}
						}
						if err := os.WriteFile(filepath.Join(dataDir, "index"), nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
					r, _ := newInitializationReceiver(t, grouped, dir, WithQueueSize(2048), WithMaxPage(3), WithQueueType(kind))
					done := make(chan error, 1)
					go func() {
						next := 0
						for {
							messages, err := r.BulkDequeue(50, time.Millisecond)
							if IsErrQueueClose(err) {
								if next != count {
									done <- fmt.Errorf("got %d items, want %d", next, count)
									return
								}
								done <- r.CloseIndex()
								return
							}
							if err != nil {
								done <- err
								return
							}
							for _, m := range messages {
								if *m.Item() != next {
									done <- fmt.Errorf("item=%d, want %d", *m.Item(), next)
									return
								}
								next++
							}
							if len(messages) > 0 {
								if err := r.UpdateIndex(messages[len(messages)-1]); err != nil {
									done <- err
									return
								}
							}
						}
					}()
					if err := r.WaitInitializeWithError(); err != nil {
						t.Fatal(err)
					}
					if err := r.WaitInitializeWithError(); err != nil {
						t.Fatal("repeated wait:", err)
					}
					if err := r.CloseQueue(); err != nil {
						t.Fatal(err)
					}
					if err := initializationResult(t, done); err != nil {
						t.Fatal(err)
					}
					if !IsErrQueueClose(r.WaitInitializeWithError()) {
						t.Fatal("closed queue accepted for startup")
					}
				})
			}
		}
	}
}

func TestWaitInitializeWithErrorFailures(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, mode := range []string{"json", "decoder-error", "decoder-panic", "ambiguous"} {
				t.Run(fmt.Sprintf("group=%t/%s/%s", grouped, kind, mode), func(t *testing.T) {
					dir := t.TempDir()
					dataDir := dir
					if grouped {
						dataDir = filepath.Join(dir, "q")
					}
					if err := os.MkdirAll(dataDir, 0700); err != nil {
						t.Fatal(err)
					}
					files := map[string][]byte{"queue.0": []byte("[1]\n"), "index": nil}
					opts := []Option{WithQueueSize(2), WithMaxPage(2), WithQueueType(kind)}
					switch mode {
					case "json":
						files["queue.0"] = []byte("[")
					case "decoder-error":
						opts = append(opts, WithDecoder(func([]byte, any) error { return errors.New("PRIVATE_DECODER_DETAIL") }))
					case "decoder-panic":
						opts = append(opts, WithDecoder(func([]byte, any) error { panic("PRIVATE_DECODER_DETAIL") }))
					case "ambiguous":
						files = map[string][]byte{"queue.0": []byte("[0,1]\n"), "queue.1": []byte("[2,3]\n"), "index": binary.LittleEndian.AppendUint64(nil, 3)}
					}
					stamp := time.Unix(1, 0)
					for name, data := range files {
						path := filepath.Join(dataDir, name)
						if err := os.WriteFile(path, data, 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.Chtimes(path, stamp, stamp); err != nil {
							t.Fatal(err)
						}
					}
					before := diskSnapshot(t, dataDir)
					r, enqueue := newInitializationReceiver(t, grouped, dir, opts...)
					started := false
					err := r.WaitInitializeWithError()
					if err == nil {
						started = true
						_ = enqueue(boundaryItems(99, 1))
					}
					if started || !isRecoveryFailure(err) || strings.Contains(err.Error(), "PRIVATE_DECODER_DETAIL") {
						t.Fatalf("startup=%t/error=%v", started, err)
					}
					if got := r.WaitInitializeWithError(); got != err {
						t.Fatalf("failure changed: %v -> %v", err, got)
					}
					r.WaitInitialize() // The legacy API still completes on failure.
					if _, got := r.Dequeue(); got != err {
						t.Fatalf("dequeue failure=%v, want %v", got, err)
					}
					if got := enqueue(boundaryItems(99, 1)); got != err {
						t.Fatalf("enqueue failure=%v, want %v", got, err)
					}
					if !reflect.DeepEqual(before, diskSnapshot(t, dataDir)) {
						t.Fatal("failure changed persisted files")
					}
				})
			}
		}
	}
}

func TestWaitInitializeWithErrorWaitersAndCancellation(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("group=%t/cancel=%t", grouped, cancel), func(t *testing.T) {
				dir := t.TempDir()
				dataDir := dir
				if grouped {
					dataDir = filepath.Join(dir, "q")
				}
				if err := os.MkdirAll(dataDir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dataDir, "queue.0"), []byte("[7]\n"), 0600); err != nil {
					t.Fatal(err)
				}
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				r, _ := newInitializationReceiver(t, grouped, dir, WithQueueSize(2), WithDecoder(func(data []byte, value any) error {
					close(entered)
					<-release
					return json.Unmarshal(data, value)
				}))
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("decoder not entered")
				}
				results := make(chan error, 12)
				var started sync.WaitGroup
				started.Add(cap(results))
				for i := 0; i < cap(results); i++ {
					go func(legacy bool) {
						started.Done()
						if legacy {
							r.WaitInitialize()
							results <- nil
						} else {
							results <- r.WaitInitializeWithError()
						}
					}(i%2 == 0)
				}
				started.Wait()
				select {
				case err := <-results:
					t.Fatalf("wait returned before initialization: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
				var closed chan error
				if cancel {
					q := initializationChild(t, r)
					closed = make(chan error, 1)
					go func() { closed <- r.CloseQueue() }()
					select {
					case <-q.stop:
					case <-time.After(3 * time.Second):
						t.Fatal("close did not cancel recovery")
					}
				}
				once.Do(func() { close(release) })
				closedErrors := 0
				for i := 0; i < cap(results); i++ {
					err := initializationResult(t, results)
					if IsErrQueueClose(err) {
						closedErrors++
					} else if err != nil {
						t.Fatal(err)
					}
				}
				if cancel {
					if closedErrors != cap(results)/2 {
						t.Fatalf("closed errors=%d", closedErrors)
					}
					if err := initializationResult(t, closed); err != nil {
						t.Fatal(err)
					}
				} else {
					if closedErrors != 0 {
						t.Fatal("open queue reported closed")
					}
					before := diskSnapshot(t, dataDir)
					if err := r.WaitInitializeWithError(); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, diskSnapshot(t, dataDir)) {
						t.Fatal("wait changed persisted files")
					}
					m, err := r.Dequeue()
					if err != nil || *m.Item() != 7 {
						t.Fatalf("wait consumed pending item: %v/%v", m, err)
					}
				}
			})
		}
	}
}

func TestWaitInitializeWithErrorRecordedFailure(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprint(grouped), func(t *testing.T) {
			r, enqueue := newInitializationReceiver(t, grouped, t.TempDir())
			if err := r.WaitInitializeWithError(); err != nil {
				t.Fatal(err)
			}
			if err := enqueue(boundaryItems(1, 1)); err != nil {
				t.Fatal(err)
			}
			m, err := r.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			q := initializationChild(t, r)
			if err := q.indexFile.Close(); err != nil {
				t.Fatal(err)
			}
			failure := r.UpdateIndex(m)
			if !isIOFailure(failure) {
				t.Fatal("index failure not injected:", failure)
			}
			if err := r.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			if got := r.WaitInitializeWithError(); got != failure {
				t.Fatalf("failure replaced by close: %v", got)
			}
			if got := q.WaitInitializeWithError(); got != failure {
				t.Fatalf("child/parent failure lost: %v", got)
			}
		})
	}
}

func TestWaitInitializeWithErrorConsumerStopsRecovery(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprint(grouped), func(t *testing.T) {
			dir := t.TempDir()
			dataDir := dir
			if grouped {
				dataDir = filepath.Join(dir, "q")
			}
			if err := os.MkdirAll(dataDir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "queue.0"), []byte("[0,1]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "queue.1"), []byte("[2,3]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dataDir, "queue.2"), []byte("[4]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			r, _ := newInitializationReceiver(t, grouped, dir, WithQueueSize(2), WithMaxPage(3))
			failed, consumerDone, proceed := make(chan error, 1), make(chan error, 1), make(chan struct{})
			failure := errors.New("business processing failed")
			go func() {
				_, err := r.Dequeue()
				if err == nil {
					err = failure
				}
				failed <- err // Notify management before waiting in CloseIndex.
				<-proceed
				consumerDone <- r.CloseIndex()
			}()
			if err := initializationResult(t, failed); err != failure {
				t.Fatal(err)
			}
			waited := make(chan error, 1)
			go func() { waited <- r.WaitInitializeWithError() }()
			if err := r.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			close(proceed)
			if err := initializationResult(t, waited); !IsErrQueueClose(err) {
				t.Fatalf("startup not canceled: %v", err)
			}
			if err := initializationResult(t, consumerDone); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(dataDir, "index")); err != nil || len(data) != 0 {
				t.Fatal("failed processing acknowledged", err)
			}
		})
	}
}

// Shift only process-local positions by whole capacity cycles. The queue files,
// circular index, and ordering of all present page ends remain consistent.
func seedSequenceNearLimit(t *testing.T, r boundaryReceiver, enqueue func([]*int) error, q *Queue[int], sequence uint64) {
	t.Helper()
	capacity := q.size * q.maxPage
	low := capacity + sequence%capacity
	if sequence < low {
		t.Fatal("sequence seed must be beyond one complete capacity cycle")
	}
	for i := uint64(0); i < low; i++ {
		value := int(i)
		if err := enqueue([]*int{&value}); err != nil {
			t.Fatal(err)
		}
		m, err := r.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.UpdateIndex(m); err != nil {
			t.Fatal(err)
		}
	}
	if q.sequence != low || q.ackNext.Load() != low {
		t.Fatal("unexpected seed progress")
	}
	before := diskSnapshot(t, q.fileDir)
	shift := sequence - low
	q.sequence += shift
	q.ackNext.Add(shift)
	for page := range q.pageEnds {
		q.pageEnds[page] += shift
	}
	if !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
		t.Fatal("seeding process-local counters changed persisted data")
	}
}

func TestSequenceLimit(t *testing.T) {
	const maximum = ^uint64(0)
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, tc := range []struct {
				name      string
				size      uint64
				remaining uint64
				count     int
				bulk      bool
			}{
				{"single exhausted at page boundary", 3, 0, 1, false},
				{"single exhausted inside page", 4, 0, 1, false},
				{"last single item", 3, 1, 1, false},
				{"bulk exhausted", 64, 0, 50, true},
				{"last bulk item", 64, 1, 1, true},
				{"bulk exceeds one remaining slot", 64, 1, 2, true},
				{"last two bulk items", 64, 2, 2, true},
				{"bulk exceeds two remaining slots", 64, 2, 3, true},
				{"bulk then single", 64, 3, 2, true},
				{"last full page", 3, 3, 3, true},
				{"bulk 50 with 49 remaining", 64, 49, 50, true},
				{"last bulk 50", 64, 50, 50, true},
				{"bulk 51 with 50 remaining", 64, 50, 51, true},
			} {
				t.Run(fmt.Sprintf("group=%t/%s/%s", grouped, kind, tc.name), func(t *testing.T) {
					r, enqueue, q := newBoundaryQueue(t, grouped, kind, tc.size, 3)
					start := maximum - tc.remaining
					seedSequenceNearLimit(t, r, enqueue, q, start)
					indexBefore, err := os.ReadFile(filepath.Join(q.fileDir, "index"))
					if err != nil {
						t.Fatal(err)
					}
					submit := func(items []*int) error {
						if tc.bulk {
							return enqueue(items)
						}
						if group, ok := r.(*GroupQueue[int]); ok {
							return group.Enqueue("q", items[0])
						}
						return q.Enqueue(items[0])
					}
					err = submit(boundaryItems(1000000, tc.count))
					if uint64(tc.count) > tc.remaining {
						if !isIOFailure(err) || !strings.Contains(err.Error(), "sequence overflow") {
							t.Fatalf("overflow result: %v", err)
						}
						if q.sequence != start || q.Length() != 0 || q.ackNext.Load() != start {
							t.Fatalf("rejected chunk changed counters: sequence=%d, ack=%d, buffered=%d", q.sequence, q.ackNext.Load(), q.Length())
						}
						data, readErr := os.ReadFile(filepath.Join(q.fileDir, "index"))
						if readErr != nil || !bytes.Equal(data, indexBefore) {
							t.Fatal("overflow changed persisted progress")
						}
						// Rotation can already have reused a completed page, but
						// no payload from the rejected chunk may have been written.
						for name, contents := range diskSnapshot(t, q.fileDir) {
							if name != "index" && strings.Contains(contents, "100000") {
								t.Fatal("overflow wrote rejected payload")
							}
						}
						if nextErr := submit(boundaryItems(2000000, 1)); nextErr != err {
							t.Fatalf("failed instance accepted/replaced failure: %v", nextErr)
						}
						if _, dequeueErr := r.Dequeue(); dequeueErr != err {
							t.Fatalf("overflow did not reach receiver: %v", dequeueErr)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if q.sequence != start+uint64(tc.count) || q.pageEnds[q.currentPage] != q.sequence {
						t.Fatal("accepted chunk did not advance safely to its expected end")
					}
					var messages []*Message[int]
					for i := 0; i < tc.count; i++ {
						m, err := r.Dequeue()
						if err != nil {
							t.Fatal(err)
						}
						if m.sequence != start+uint64(i) || m.sequence == maximum || m.Index() != m.sequence%(q.size*q.maxPage) || *m.Item() != 1000000+i {
							t.Fatalf("unexpected message at upper limit: sequence=%d, index=%d, item=%d", m.sequence, m.Index(), *m.Item())
						}
						if err := r.UpdateIndex(m); err != nil {
							t.Fatal(err)
						}
						messages = append(messages, m)
					}
					for q.sequence < maximum {
						if err := submit(boundaryItems(2000000, 1)); err != nil {
							t.Fatal(err)
						}
						m, err := r.Dequeue()
						if err != nil {
							t.Fatal(err)
						}
						if err := r.UpdateIndex(m); err != nil {
							t.Fatal(err)
						}
					}
					for i, m := range messages {
						if m.sequence != start+uint64(i) {
							t.Fatal("previous Message.sequence changed as Queue.sequence advanced")
						}
					}
					before := diskSnapshot(t, q.fileDir)
					invalid := &Message[int]{owner: q, sequence: maximum, index: maximum % (q.size * q.maxPage)}
					if err := r.UpdateIndex(invalid); !isProgressFailure(err) {
						t.Fatalf("maximum message sequence was not rejected: %v", err)
					}
					if q.ackNext.Load() != maximum || !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
						t.Fatal("sequence+1 wrapped progress or modified index")
					}
					if err := enqueue(nil); err != nil || q.sequence != maximum {
						t.Fatalf("empty bulk at limit: %v, %d", err, q.sequence)
					}
					if err := submit(boundaryItems(3000000, 1)); !isIOFailure(err) || q.sequence != maximum {
						t.Fatalf("next item wrapped instead of failing: %v, %d", err, q.sequence)
					}
				})
			}
		}
	}
}

func TestSequenceLimitProtectsPendingPage(t *testing.T) {
	const maximum = ^uint64(0)
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			t.Run(fmt.Sprintf("group=%t/%s", grouped, kind), func(t *testing.T) {
				r, enqueue, q := newBoundaryQueue(t, grouped, kind, 3, 2)
				seedSequenceNearLimit(t, r, enqueue, q, maximum-7)
				var held []*Message[int]
				for i := 0; i < 4; i++ {
					if err := enqueue(boundaryItems(1000000+i, 1)); err != nil {
						t.Fatal(err)
					}
					m, err := r.Dequeue()
					if err != nil {
						t.Fatal(err)
					}
					held = append(held, m)
				}
				before := diskSnapshot(t, q.fileDir)
				done := make(chan error, 1)
				go func() { done <- enqueue(boundaryItems(2000000, 3)) }()
				waitForPageProtection(t)
				if !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
					t.Fatal("pending page was reused before its last item was acknowledged")
				}
				if err := r.UpdateIndex(held[0]); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if q.sequence != maximum || q.pageEnds[0] != maximum || q.ackNext.Load() != maximum-6 {
					t.Fatal("page end/progress wrapped at the sequence limit")
				}
				if after := diskSnapshot(t, q.fileDir); after["queue.1"] != before["queue.1"] {
					t.Fatal("another pending page was modified")
				}
				for _, m := range held[1:] {
					if err := r.UpdateIndex(m); err != nil {
						t.Fatal(err)
					}
				}
				for i := 0; i < 3; i++ {
					m, err := r.Dequeue()
					if err != nil {
						t.Fatal(err)
					}
					if m.sequence != maximum-3+uint64(i) || *m.Item() != 2000000+i {
						t.Fatal("last page lost or reordered messages")
					}
					if err := r.UpdateIndex(m); err != nil {
						t.Fatal(err)
					}
				}
				if q.ackNext.Load() != maximum {
					t.Fatal("final acknowledgement wrapped")
				}
			})
		}
	}
}

func TestSequenceMaximumMessageRejected(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			t.Run(fmt.Sprintf("group=%t/%s", grouped, kind), func(t *testing.T) {
				r, enqueue, q := newBoundaryQueue(t, grouped, kind, 3, 2)
				before := diskSnapshot(t, q.fileDir)
				// Public enqueues cannot produce this sequence. Exercise the
				// defensive sequence+1 check with initial ackNext=0, where an
				// unchecked wrap could otherwise look like a duplicate update.
				invalid := &Message[int]{owner: q, sequence: ^uint64(0), index: 3}
				if err := r.UpdateIndex(invalid); !isProgressFailure(err) {
					t.Fatalf("maximum sequence accepted at initial progress: %v", err)
				}
				if q.sequence != 0 || q.ackNext.Load() != 0 || !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
					t.Fatal("invalid sequence modified initial state")
				}
				if err := enqueue(boundaryItems(1000000, 1)); err != nil {
					t.Fatal(err)
				}
				m, err := r.Dequeue()
				if err != nil || m.sequence != 0 {
					t.Fatal("invalid update damaged subsequent normal progress")
				}
				if err := r.UpdateIndex(m); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestSequenceLimitConcurrentProducers(t *testing.T) {
	const maximum = ^uint64(0)
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("group=%t", grouped), func(t *testing.T) {
			r, enqueue, q := newBoundaryQueue(t, grouped, MPSC, 3, 2)
			seedSequenceNearLimit(t, r, enqueue, q, maximum-2)
			var wg sync.WaitGroup
			results := make(chan struct {
				value int
				err   error
			}, 4)
			for producer := 0; producer < 4; producer++ {
				wg.Add(1)
				go func(producer int) {
					defer wg.Done()
					value := 1000000 + producer
					var err error
					if group, ok := r.(*GroupQueue[int]); ok {
						err = group.Enqueue("q", &value)
					} else {
						err = q.Enqueue(&value)
					}
					results <- struct {
						value int
						err   error
					}{value, err}
				}(producer)
			}
			wg.Wait()
			close(results)
			accepted := map[int]bool{}
			for result := range results {
				if result.err == nil {
					accepted[result.value] = true
				} else if !isIOFailure(result.err) || !strings.Contains(result.err.Error(), "sequence overflow") {
					t.Fatalf("unexpected producer failure: %v", result.err)
				}
			}
			if len(accepted) != 2 || q.sequence != maximum || q.Length() != 2 || q.ackNext.Load() != maximum-2 {
				t.Fatalf("concurrent limit: accepted=%d, sequence=%d, buffered=%d, ack=%d", len(accepted), q.sequence, q.Length(), q.ackNext.Load())
			}
			// Failure stops public dequeue. Inspect buffered messages only after
			// all producers finish, to verify exactly which writes succeeded.
			for i := 0; i < 2; i++ {
				m := <-q.queue
				if !accepted[*m.Item()] || m.sequence != maximum-2+uint64(i) {
					t.Fatal("concurrent producers published duplicates or wrapped sequences")
				}
				delete(accepted, *m.Item())
			}
		})
	}
}

func TestSequenceReconstructedOnReopen(t *testing.T) {
	const maximum = ^uint64(0)
	for _, kind := range []QueueType{SPSC, MPSC} {
		t.Run(string(kind), func(t *testing.T) {
			r, enqueue, q := newBoundaryQueue(t, false, kind, 4, 2)
			seedSequenceNearLimit(t, r, enqueue, q, maximum-2)
			if err := enqueue(boundaryItems(1000000, 2)); err != nil {
				t.Fatal(err)
			}
			var old *Message[int]
			for i := 0; i < 2; i++ {
				var err error
				old, err = r.Dequeue()
				if err != nil {
					t.Fatal(err)
				}
				if err := r.UpdateIndex(old); err != nil {
					t.Fatal(err)
				}
			}
			if err := enqueue(boundaryItems(2000000, 1)); !isIOFailure(err) {
				t.Fatal("expected sequence exhaustion")
			}
			if err := r.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			if err := r.CloseIndex(); err != nil {
				t.Fatal(err)
			}
			before := diskSnapshot(t, q.fileDir)
			reopened, err := NewQueue[int]("q", WithFileDir(q.fileDir), WithQueueSize(4), WithMaxPage(2), WithQueueType(kind))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reopened.CloseQueue(); reopened.CloseIndex() })
			if err := reopened.waitInitialize(); err != nil {
				t.Fatal(err)
			}
			if reopened.sequence != 7 || reopened.ackNext.Load() != 7 || reopened.Length() != 0 || !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
				t.Fatal("reopen retained lifetime sequence or changed persisted data")
			}
			if err := reopened.UpdateIndex(old); !isProgressFailure(err) {
				t.Fatal("old Message from another open was accepted")
			}
			value := 3000000
			if err := reopened.Enqueue(&value); err != nil {
				t.Fatal(err)
			}
			m, err := reopened.Dequeue()
			if err != nil || m.sequence != 7 || m.Index() != 7 {
				t.Fatalf("reopened order: %v, %v", m, err)
			}
			if err := reopened.UpdateIndex(m); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Boundary tests use the same public dequeue/progress operations for both
// queue types. Fixtures start empty; preloaded closed cases fit in one page.
type boundaryReceiver interface {
	Dequeue() (*Message[int], error)
	BulkDequeue(uint64, time.Duration) ([]*Message[int], error)
	FuncAfterBulkDequeue(uint64, time.Duration, func([]*int) error) (int, error)
	UpdateIndex(*Message[int]) error
	CloseQueue() error
	CloseIndex() error
}

func newBoundaryQueue(t *testing.T, grouped bool, kind QueueType, size, pages uint64) (boundaryReceiver, func([]*int) error, *Queue[int]) {
	t.Helper()
	opts := []Option{WithFileDir(t.TempDir()), WithQueueSize(size), WithMaxPage(pages), WithQueueType(kind)}
	var receiver boundaryReceiver
	var enqueue func([]*int) error
	var child *Queue[int]
	if grouped {
		group, err := NewGroupQueue[int]("g", opts...)
		if err != nil {
			t.Fatal(err)
		}
		receiver = group
		t.Cleanup(func() { group.CloseQueue(); group.CloseIndex() })
		if err := group.waitInitialize(); err != nil {
			t.Fatal(err)
		}
		if err := group.addQueue("q"); err != nil {
			t.Fatal(err)
		}
		child, err = group.getQueue("q")
		if err != nil {
			t.Fatal(err)
		}
		enqueue = func(items []*int) error { return group.BulkEnqueue("q", items) }
	} else {
		var err error
		child, err = NewQueue[int]("q", opts...)
		if err != nil {
			t.Fatal(err)
		}
		receiver, enqueue = child, child.BulkEnqueue
		t.Cleanup(func() { child.CloseQueue(); child.CloseIndex() })
		if err := child.waitInitialize(); err != nil {
			t.Fatal(err)
		}
	}
	return receiver, enqueue, child
}

func boundaryItems(start, count int) []*int {
	items := make([]*int, count)
	for i := range items {
		value := start + i
		items[i] = &value
	}
	return items
}

func TestBoundaryBulkDequeue(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, callback := range []bool{false, true} {
				for _, limit := range []int{0, 1, 2, 49, 50, 51, 63, 64, 65} {
					counts := map[int]bool{}
					// Keep case ordering deterministic, independently of -shuffle.
					for _, available := range []int{0, 1, limit - 1, limit, limit + 1} {
						if available < 0 || counts[available] {
							continue
						}
						counts[available] = true
						t.Run(fmt.Sprintf("group=%t/%s/callback=%t/limit=%d/available=%d", grouped, kind, callback, limit, available), func(t *testing.T) {
							r, enqueue, child := newBoundaryQueue(t, grouped, kind, 66, 3)
							if err := enqueue(boundaryItems(0, available)); err != nil {
								t.Fatal(err)
							}
							if err := r.CloseQueue(); err != nil {
								t.Fatal(err)
							}
							var values []int
							var err error
							if callback {
								var count int
								count, err = r.FuncAfterBulkDequeue(uint64(limit), time.Second, func(items []*int) error {
									for _, item := range items {
										values = append(values, *item)
									}
									return nil
								})
								if count != len(values) {
									t.Errorf("reported count %d, callback received %d", count, len(values))
								}
							} else {
								var messages []*Message[int]
								messages, err = r.BulkDequeue(uint64(limit), time.Second)
								for _, m := range messages {
									values = append(values, *m.Item())
									if err := r.UpdateIndex(m); err != nil {
										t.Fatal(err)
									}
								}
							}
							if limit == 0 && !IsErrQueueOption(err) {
								t.Fatalf("zero bulk size: %v", err)
							}
							if limit > 0 && err != nil && !(available == 0 && IsErrQueueClose(err)) {
								t.Fatalf("unexpected bulk error: %v", err)
							}
							want := available
							if want > limit {
								want = limit
							}
							if len(values) != want {
								t.Errorf("bulk returned/processed %d items; maximum=%d, available=%d, want=%d", len(values), limit, available, want)
							}
							if child.Length() != uint64(available-len(values)) {
								t.Fatal("bulk consumed items absent from its result")
							}
							if limit == 0 && callback && child.ackNext.Load() != 0 {
								t.Error("zero-limit call advanced persisted progress")
							}
							// Check conservation as well as the first batch's upper bound.
							for {
								m, err := r.Dequeue()
								if IsErrQueueClose(err) {
									break
								}
								if err != nil {
									t.Fatal(err)
								}
								values = append(values, *m.Item())
								if err := r.UpdateIndex(m); err != nil {
									t.Fatal(err)
								}
							}
							if len(values) != available {
								t.Fatalf("lost/duplicated items: got %d, want %d", len(values), available)
							}
							for i, value := range values {
								if value != i {
									t.Fatalf("item %d = %d", i, value)
								}
							}
							if err := r.CloseIndex(); err != nil {
								t.Fatal(err)
							}
						})
					}
				}
			}
		}
	}
}

func TestBulkDequeueOpenBoundaries(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, callback := range []bool{false, true} {
				for _, limit := range []uint64{0, 1} {
					for _, available := range []int{0, 1, 2} {
						if limit == 1 && available == 0 {
							continue // A positive limit must wait for its first item.
						}
						t.Run(fmt.Sprintf("group=%t/%s/callback=%t/limit=%d/available=%d", grouped, kind, callback, limit, available), func(t *testing.T) {
							r, enqueue, q := newBoundaryQueue(t, grouped, kind, 4, 2)
							if err := enqueue(boundaryItems(0, available)); err != nil {
								t.Fatal(err)
							}
							before := diskSnapshot(t, q.fileDir)
							type result struct {
								messages []*Message[int]
								values   []int
								calls    int
								err      error
							}
							done := make(chan result, 1)
							go func() {
								var got result
								if callback {
									_, got.err = r.FuncAfterBulkDequeue(limit, time.Hour, func(items []*int) error {
										got.calls++
										for _, item := range items {
											got.values = append(got.values, *item)
										}
										return nil
									})
								} else {
									got.messages, got.err = r.BulkDequeue(limit, time.Hour)
									for _, m := range got.messages {
										got.values = append(got.values, *m.Item())
									}
								}
								done <- got
							}()
							var got result
							select {
							case got = <-done:
							case <-time.After(250 * time.Millisecond):
								t.Error("bulk call waited for lazy timeout or queue close after reaching its limit")
								r.CloseQueue()
								got = <-done
							}
							if len(got.values) != int(limit) || q.Length() != uint64(available)-limit {
								t.Errorf("returned=%v, remaining=%d; limit=%d, available=%d", got.values, q.Length(), limit, available)
							}
							if limit == 0 {
								if !IsErrQueueOption(got.err) || got.calls != 0 || !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
									t.Errorf("zero-size call had side effects: error=%v, callback calls=%d", got.err, got.calls)
								}
							} else {
								if got.err != nil || len(got.values) != 1 || got.values[0] != 0 || callback && got.calls != 1 {
									t.Errorf("one-item result: %+v", got)
								}
								for _, m := range got.messages {
									if err := r.UpdateIndex(m); err != nil {
										t.Fatal(err)
									}
								}
							}
							r.CloseQueue()
							for {
								m, err := r.Dequeue()
								if IsErrQueueClose(err) {
									break
								}
								if err != nil {
									t.Fatal(err)
								}
								if err := r.UpdateIndex(m); err != nil {
									t.Fatal(err)
								}
							}
							if err := r.CloseIndex(); err != nil {
								t.Fatal(err)
							}
						})
					}
				}
			}
		}
	}
}

func TestBoundaryBulkCallbackErrors(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, count := range []int{1, 2, 50} {
				for _, failure := range []string{"none", "callback", "index", "both"} {
					t.Run(fmt.Sprintf("group=%t/%s/count=%d/failure=%s", grouped, kind, count, failure), func(t *testing.T) {
						r, enqueue, child := newBoundaryQueue(t, grouped, kind, 64, 3)
						if err := enqueue(boundaryItems(0, count)); err != nil {
							t.Fatal(err)
						}
						if err := r.CloseQueue(); err != nil {
							t.Fatal(err)
						}
						indexFailure := failure == "index" || failure == "both"
						callbackFailure := failure == "callback" || failure == "both"
						if indexFailure {
							if err := child.CloseIndex(); err != nil {
								t.Fatal(err)
							}
						}
						injected := errors.New("boundary callback failure")
						calls := 0
						n, err := r.FuncAfterBulkDequeue(uint64(count), time.Second, func(items []*int) error {
							calls++
							if len(items) != count {
								t.Errorf("callback received %d, want %d", len(items), count)
							}
							for i, item := range items {
								if *item != i {
									t.Errorf("item %d = %d", i, *item)
								}
							}
							if callbackFailure {
								return injected
							}
							return nil
						})
						if n != count || calls != 1 {
							t.Errorf("processed=%d, callback calls=%d; want %d, 1", n, calls, count)
						}
						if callbackFailure && !errors.Is(err, injected) {
							t.Errorf("callback error was lost: %v", err)
						}
						if indexFailure && !errors.Is(err, os.ErrClosed) {
							t.Errorf("index error was lost: %v", err)
						}
						if failure == "none" && err != nil {
							t.Fatal(err)
						}
						wantAck := uint64(count)
						if indexFailure {
							wantAck = 0
						}
						// Preserve the existing acknowledge-after-callback behavior,
						// including callbacks returning an error; only report it.
						if child.ackNext.Load() != wantAck {
							t.Errorf("progress=%d, want %d", child.ackNext.Load(), wantAck)
						}
					})
				}
			}
		}
	}
}

func TestBoundaryBulkEnqueuePages(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, size := range []int{1, 2, 3, 2048} {
				if grouped && size == 2048 {
					continue // The production configuration is SimpleQueue.
				}
				for _, pages := range []int{2, 3} {
					if size == 2048 && pages != 3 {
						continue
					}
					capacity := size * pages
					cases := [][2]int{{0, 0}, {0, 1}, {0, size - 1}, {0, size}, {0, size + 1}, {0, capacity - 1}, {0, capacity}, {0, capacity + 1}, {size - 1, 2}, {capacity - 1, 2}}
					seen := map[[2]int]bool{}
					for _, tc := range cases {
						if seen[tc] {
							continue
						}
						seen[tc] = true
						t.Run(fmt.Sprintf("group=%t/%s/size=%d/pages=%d/start=%d/count=%d", grouped, kind, size, pages, tc[0], tc[1]), func(t *testing.T) {
							r, enqueue, child := newBoundaryQueue(t, grouped, kind, uint64(size), uint64(pages))
							total := tc[0] + tc[1]
							done := make(chan error, 1)
							finish := func(err error) {
								if err != nil {
									child.fail(err)
								}
								done <- err
							}
							go func() {
								next := 0
								for {
									m, err := r.Dequeue()
									if IsErrQueueClose(err) {
										if next != total {
											finish(fmt.Errorf("received %d, want %d", next, total))
											return
										}
										finish(r.CloseIndex())
										return
									}
									if err != nil {
										finish(err)
										return
									}
									if *m.Item() != next || m.Index() != uint64(next%capacity) {
										finish(fmt.Errorf("item/index=%d/%d, want %d/%d", *m.Item(), m.Index(), next, next%capacity))
										return
									}
									if err := r.UpdateIndex(m); err != nil {
										finish(err)
										return
									}
									next++
								}
							}()
							// The consumer remains active while each potentially
							// capacity-spanning producer batch finishes.
							for _, batch := range [][2]int{{0, tc[0]}, {tc[0], tc[1]}} {
								if err := enqueue(boundaryItems(batch[0], batch[1])); err != nil {
									t.Fatal(err)
								}
							}
							if err := r.CloseQueue(); err != nil {
								t.Fatal(err)
							}
							if err := <-done; err != nil {
								t.Fatal(err)
							}
							data, err := os.ReadFile(filepath.Join(child.fileDir, "index"))
							if err != nil {
								t.Fatal(err)
							}
							if total == 0 {
								if len(data) != 0 {
									t.Fatalf("empty batch changed index: %x", data)
								}
							} else if len(data) != 8 || binary.LittleEndian.Uint64(data) != uint64((total-1)%capacity) {
								t.Fatalf("wrong persisted progress: %x", data)
							}
							for name, content := range diskSnapshot(t, child.fileDir) {
								if name == "index" {
									continue
								}
								if !strings.HasPrefix(name, "queue.") {
									t.Fatalf("unexpected metadata file: %s", name)
								}
								count := 0
								for _, line := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
									if line == "" && content == "" {
										continue
									}
									var values []int
									if err := json.Unmarshal([]byte(line), &values); err != nil || len(values) == 0 {
										t.Fatalf("invalid persisted array: %q, %v", line, err)
									}
									count += len(values)
								}
								if count > size {
									t.Fatalf("%s contains %d items, page limit %d", name, count, size)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestBoundaryRecoveryHistories(t *testing.T) {
	for _, config := range [][2]int{{1, 2}, {2, 2}, {3, 3}} {
		size, pages := config[0], config[1]
		capacity := size * pages
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, bulkRecords := range []bool{false, true} {
				for head := 0; head <= 2*capacity+1; head++ {
					firstRetained := 0
					if head > capacity {
						firstRetained = ((head-1)/size - pages + 1) * size
					}
					for completed := firstRetained; completed <= head; completed++ {
						t.Run(fmt.Sprintf("%s/bulk=%t/size=%d/pages=%d/written=%d/completed=%d", kind, bulkRecords, size, pages, head, completed), func(t *testing.T) {
							dir := t.TempDir()
							files := map[string]string{}
							valuesByPage := map[string][]int{}
							for value := 0; value < head; value++ {
								name := fmt.Sprintf("queue.%d", (value/size)%pages)
								if value%size == 0 {
									files[name] = ""
									valuesByPage[name] = nil
								}
								files[name] += fmt.Sprintf("[%d]\n", value)
								valuesByPage[name] = append(valuesByPage[name], value)
							}
							if bulkRecords {
								for name, values := range valuesByPage {
									data, err := json.Marshal(values)
									if err != nil {
										t.Fatal(err)
									}
									files[name] = string(data) + "\n"
								}
							}
							for name, contents := range files {
								if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
									t.Fatal(err)
								}
							}
							var index []byte
							if completed > 0 {
								index = binary.LittleEndian.AppendUint64(nil, uint64((completed-1)%capacity))
							}
							if err := os.WriteFile(filepath.Join(dir, "index"), index, 0600); err != nil {
								t.Fatal(err)
							}
							// These histories have reliable, distinguishable times.
							// Existing tests separately require ambiguous histories to fail.
							stampKnownHistory(t, dir, uint64(size), uint64(pages), uint64(completed), uint64(head-completed))
							q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(uint64(size)), WithMaxPage(uint64(pages)), WithQueueType(kind))
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { q.CloseQueue(); q.CloseIndex() })
							got, err := drainCompatibilityQueue(t, q)
							if err != nil || len(got) != head-completed {
								t.Fatalf("recovery: got %v, error %v; want values [%d,%d)", got, err, completed, head)
							}
							for i, value := range got {
								if value != completed+i {
									t.Fatalf("recovered item %d = %d, want %d", i, value, completed+i)
								}
							}
							for name, contents := range files {
								data, err := os.ReadFile(filepath.Join(dir, name))
								if err != nil || string(data) != contents {
									t.Fatalf("recovery changed %s: %v", name, err)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestBoundaryIndexFiles(t *testing.T) {
	for _, kind := range []QueueType{SPSC, MPSC} {
		for _, length := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 16} {
			for _, value := range []uint64{0, 1, 6, 7, 8, 9, ^uint64(0)} {
				if length != 8 && value != 0 {
					continue
				}
				t.Run(fmt.Sprintf("%s/length=%d/value=%d", kind, length, value), func(t *testing.T) {
					dir := t.TempDir()
					for page := 0; page < 2; page++ {
						data, _ := json.Marshal([]int{page*4 + 0, page*4 + 1, page*4 + 2, page*4 + 3})
						path := filepath.Join(dir, fmt.Sprintf("queue.%d", page))
						if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
							t.Fatal(err)
						}
						stamp := time.Unix(int64(10+page*10), 0)
						if err := os.Chtimes(path, stamp, stamp); err != nil {
							t.Fatal(err)
						}
					}
					data := make([]byte, length)
					if length == 8 {
						binary.LittleEndian.PutUint64(data, value)
					}
					path := filepath.Join(dir, "index")
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					stamp := time.Unix(30, 0)
					if err := os.Chtimes(path, stamp, stamp); err != nil {
						t.Fatal(err)
					}
					before := diskSnapshot(t, dir)
					q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(4), WithMaxPage(2), WithQueueType(kind))
					valid := length == 0 || length == 8 && value < 8
					if !valid {
						if err == nil {
							q.CloseQueue()
							q.CloseIndex()
							t.Fatal("invalid index accepted")
						}
						if !reflect.DeepEqual(diskSnapshot(t, dir), before) {
							t.Fatal("invalid index was changed")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { q.CloseQueue(); q.CloseIndex() })
					got, err := drainCompatibilityQueue(t, q)
					start := 0
					if length == 8 {
						start = int(value) + 1
					}
					if err != nil || len(got) != 8-start {
						t.Fatalf("recovered %v, error %v, want [%d,8)", got, err, start)
					}
					for i, value := range got {
						if value != start+i {
							t.Fatalf("recovered value %d, want %d", value, start+i)
						}
					}
				})
			}
		}
	}
}

func TestBoundaryRecoveryCycleEndIndex(t *testing.T) {
	// Write actual queue files through the API, rather than relying only on
	// generated histories. The last acknowledged index is capacity-1 and the
	// final page of the next cycle contains an incomplete page of pending data.
	for _, kind := range []QueueType{SPSC, MPSC} {
		for _, config := range [][2]int{{2, 2}, {2048, 3}} {
			size, pages := config[0], config[1]
			completed := size * pages
			pending := (pages-1)*size + 1
			t.Run(fmt.Sprintf("%s/size=%d/pages=%d", kind, size, pages), func(t *testing.T) {
				r, enqueue, q := newBoundaryQueue(t, false, kind, uint64(size), uint64(pages))
				done := make(chan error, 1)
				finish := func(err error) {
					if err != nil {
						q.fail(err)
					}
					done <- err
				}
				go func() {
					next := 0
					for {
						m, err := r.Dequeue()
						if IsErrQueueClose(err) {
							if next != completed+pending {
								finish(fmt.Errorf("written history lost items: %d", next))
								return
							}
							finish(r.CloseIndex())
							return
						}
						if err != nil {
							finish(err)
							return
						}
						if *m.Item() != next {
							finish(fmt.Errorf("item=%d, want %d", *m.Item(), next))
							return
						}
						if next < completed {
							if err := r.UpdateIndex(m); err != nil {
								finish(err)
								return
							}
						}
						next++
					}
				}()
				if err := enqueue(boundaryItems(0, completed+pending)); err != nil {
					t.Fatal(err)
				}
				if err := r.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				before := diskSnapshot(t, q.fileDir)
				data := []byte(before["index"])
				if len(data) != 8 || binary.LittleEndian.Uint64(data) != uint64(completed-1) {
					t.Fatalf("unexpected persisted cycle-end index: %x", data)
				}
				reopened, err := NewQueue[int]("q", WithFileDir(q.fileDir), WithQueueSize(uint64(size)), WithMaxPage(uint64(pages)), WithQueueType(kind))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { reopened.CloseQueue(); reopened.CloseIndex() })
				got, err := drainCompatibilityQueue(t, reopened)
				if err != nil {
					if !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
						t.Error("failed recovery modified retained files")
					}
					t.Fatalf("valid cycle-end history rejected: %v; want %d pending items", err, pending)
				}
				if len(got) != pending {
					t.Fatalf("recovered %d, want %d", len(got), pending)
				}
				for i, value := range got {
					if value != completed+i {
						t.Fatalf("recovered item %d = %d, want %d", i, value, completed+i)
					}
				}
			})
		}
	}
}

func TestBoundaryProgressOrderAndClose(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			t.Run(fmt.Sprintf("group=%t/%s", grouped, kind), func(t *testing.T) {
				r, enqueue, child := newBoundaryQueue(t, grouped, kind, 1, 2)
				var first *Message[int]
				for i := 0; i < 6; i++ {
					if err := enqueue(boundaryItems(i, 1)); err != nil {
						t.Fatal(err)
					}
					m, err := r.Dequeue()
					if err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						first = m
					}
					if m.Index() != uint64(i%2) || *m.Item() != i {
						t.Fatalf("item/index=%d/%d, want %d/%d", *m.Item(), m.Index(), i, i%2)
					}
					if err := r.UpdateIndex(m); err != nil {
						t.Fatal(err)
					}
					before := diskSnapshot(t, child.fileDir)
					if err := r.UpdateIndex(m); err != nil {
						t.Fatalf("duplicate latest update: %v", err)
					}
					if i > 0 {
						if err := r.UpdateIndex(first); !isProgressFailure(err) {
							t.Fatalf("old message accepted across index wrap: %v", err)
						}
					}
					if !reflect.DeepEqual(diskSnapshot(t, child.fileDir), before) {
						t.Fatal("duplicate/backward update changed disk contents")
					}
				}
				if err := r.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				if err := r.CloseQueue(); err != nil {
					t.Fatalf("repeated queue close: %v", err)
				}
				before := diskSnapshot(t, child.fileDir)
				if err := enqueue(boundaryItems(6, 1)); err == nil {
					t.Fatal("enqueue after close succeeded")
				}
				if err := enqueue(nil); err == nil {
					t.Fatal("empty enqueue after close bypassed closed-state check")
				}
				if _, err := r.Dequeue(); !IsErrQueueClose(err) {
					t.Fatalf("closed empty queue: %v", err)
				}
				if err := r.CloseIndex(); err != nil {
					t.Fatal(err)
				}
				if err := r.CloseIndex(); err != nil {
					t.Fatalf("repeated index close: %v", err)
				}
				if !reflect.DeepEqual(diskSnapshot(t, child.fileDir), before) {
					t.Fatal("closed-queue operations changed retained data")
				}
			})
		}
	}
}

// Storage format compatibility.

func TestRecoveryRecordValidationAndPreservation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		index *uint64
		size  uint64
		want  []int
	}{
		{name: "whitespace after array", files: map[string]string{"queue.0": "  [1,2] \t\r\n"}, size: 4, want: []int{1, 2}},
		{name: "array at EOF", files: map[string]string{"queue.0": "[1,2]"}, size: 4, want: []int{1, 2}},
		{name: "second JSON value", files: map[string]string{"queue.0": "[1] [2]\n"}, size: 4},
		{name: "garbage after array", files: map[string]string{"queue.0": "[1] garbage\n"}, size: 4},
		{name: "extra JSON in completed record", files: map[string]string{"queue.0": "[0] [999]\n"}, index: new(uint64), size: 4},
		{name: "garbage in completed record", files: map[string]string{"queue.0": "[0] garbage\n"}, index: new(uint64), size: 4},
		{name: "valid prefix before invalid record", files: map[string]string{"queue.0": "[1]\n[2] garbage\n"}, size: 4},
		{name: "not an array", files: map[string]string{"queue.0": "{\"value\":1}\n"}, size: 4},
		{name: "blank record", files: map[string]string{"queue.0": " \t\n"}, size: 4},
		{name: "noncanonical page name", files: map[string]string{"queue.01": "[1]\n"}, size: 4},
		{name: "out of range page", files: map[string]string{"queue.3": "[1]\n"}, size: 4},
		{name: "multiple partial pages", files: map[string]string{"queue.0": "[1]\n", "queue.1": "[4]\n"}, size: 4},
		{name: "index without data", files: map[string]string{"queue.0": ""}, index: new(uint64), size: 4},
		{name: "index beyond retained data", files: map[string]string{"queue.0": "[0]\n"}, index: func() *uint64 { n := uint64(8); return &n }(), size: 4},
		{name: "cycle-end index with missing retained pages", files: map[string]string{"queue.0": "[0]\n"}, index: func() *uint64 { n := uint64(11); return &n }(), size: 4},
		{name: "cycle-end index with empty retained page", files: map[string]string{"queue.0": "", "queue.1": "[2,3]\n", "queue.2": "[4]\n"}, index: func() *uint64 { n := uint64(5); return &n }(), size: 2},
		{name: "missing pending page after valid prefix", files: map[string]string{"queue.0": "[0,1]\n", "queue.2": "[4]\n"}, index: new(uint64), size: 2},
		{name: "ambiguous completion on partial page", files: map[string]string{"queue.0": "[6]\n", "queue.1": "[2,3]\n", "queue.2": "[4,5]\n"}, index: new(uint64), size: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			index := []byte{}
			if tc.index != nil {
				index = binary.LittleEndian.AppendUint64(nil, *tc.index)
			}
			if err := os.WriteFile(filepath.Join(dir, "index"), index, 0600); err != nil {
				t.Fatal(err)
			}
			for name, content := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// Distinguish page history, except the intentional completion tie.
			for name := range tc.files {
				stamp := time.Unix(10, 0)
				if name == "queue.0" {
					stamp = time.Unix(30, 0)
				}
				if err := os.Chtimes(filepath.Join(dir, name), stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			stamp := time.Unix(30, 0)
			if err := os.Chtimes(filepath.Join(dir, "index"), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			before := diskSnapshot(t, dir)
			q, err := NewQueue[int]("validation", WithFileDir(dir), WithQueueSize(tc.size), WithMaxPage(3))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { q.CloseQueue(); q.CloseIndex() })
			got, err := drainCompatibilityQueue(t, q)
			if tc.want != nil {
				if err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("valid record: values=%v, error=%v", got, err)
				}
				for name, content := range tc.files {
					data, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil || string(data) != content {
						t.Fatalf("valid payload changed: %s, %v", name, err)
					}
				}
				return
			}
			if !isRecoveryFailure(err) || len(got) != 0 || q.Length() != 0 {
				t.Fatalf("invalid recovery exposed data: values=%v, buffered=%d, error=%v", got, q.Length(), err)
			}
			if !reflect.DeepEqual(diskSnapshot(t, dir), before) {
				t.Fatal("failed recovery modified retained data or progress")
			}
		})
	}
}

func TestCompletedRecordsAllowTypeChange(t *testing.T) {
	type previousType struct {
		Value string `json:"value"`
	}
	for _, kind := range []QueueType{SPSC, MPSC} {
		for _, bulk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/bulk=%t", kind, bulk), func(t *testing.T) {
				dir := t.TempDir()
				decodeCalls := 0
				decoder := func(data []byte, target any) error {
					decodeCalls++
					return json.Unmarshal(data, target)
				}
				opts := []Option{WithFileDir(dir), WithQueueSize(8), WithMaxPage(3), WithQueueType(kind), WithDecoder(decoder)}
				previous, err := NewQueue[previousType]("types", opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { previous.CloseQueue(); previous.CloseIndex() })
				if err := previous.waitInitialize(); err != nil {
					t.Fatal(err)
				}
				oldValues := []*previousType{{Value: "a"}, {Value: "b"}, {Value: "c"}}
				if bulk {
					if err := previous.BulkEnqueue(oldValues); err != nil {
						t.Fatal(err)
					}
				} else {
					for _, value := range oldValues {
						if err := previous.Enqueue(value); err != nil {
							t.Fatal(err)
						}
					}
				}
				for _, want := range oldValues {
					m, err := previous.Dequeue()
					if err != nil {
						t.Fatal(err)
					}
					if *m.Item() != *want {
						t.Fatal("previous-type order changed")
					}
					if err := previous.UpdateIndex(m); err != nil {
						t.Fatal(err)
					}
				}
				if err := previous.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				if err := previous.CloseIndex(); err != nil {
					t.Fatal(err)
				}
				oldBytes, err := os.ReadFile(filepath.Join(dir, "queue.0"))
				if err != nil {
					t.Fatal(err)
				}
				// Objects from previousType cannot be decoded as int. Completed
				// records must only undergo the type-independent JSON validation.
				current, err := NewQueue[int]("types", opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { current.CloseQueue(); current.CloseIndex() })
				if err := current.waitInitialize(); err != nil {
					t.Fatal(err)
				}
				if decodeCalls != 0 || current.Length() != 0 {
					t.Fatalf("completed old records were decoded: calls=%d, length=%d", decodeCalls, current.Length())
				}
				a, b := 40, 41
				if bulk {
					if err := current.BulkEnqueue([]*int{&a, &b}); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := current.Enqueue(&a); err != nil {
						t.Fatal(err)
					}
					if err := current.Enqueue(&b); err != nil {
						t.Fatal(err)
					}
				}
				// Leave only new-type records pending, in the same physical page.
				if err := current.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				if err := current.CloseIndex(); err != nil {
					t.Fatal(err)
				}
				mixed, err := os.ReadFile(filepath.Join(dir, "queue.0"))
				if err != nil || !bytes.HasPrefix(mixed, oldBytes) || len(mixed) <= len(oldBytes) {
					t.Fatalf("old records were changed instead of appending new records: %v", err)
				}
				reopened, err := NewQueue[int]("types", opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { reopened.CloseQueue(); reopened.CloseIndex() })
				got, err := drainCompatibilityQueue(t, reopened)
				if err != nil || !reflect.DeepEqual(got, []int{40, 41}) {
					t.Fatalf("new-type recovery: got %v, error %v", got, err)
				}
				wantCalls := 2
				if bulk {
					wantCalls = 1
				}
				if decodeCalls != wantCalls {
					t.Fatalf("decoded %d records, want only %d new-type records", decodeCalls, wantCalls)
				}
			})
		}
	}
}

// Captured from actual v0.2.1 and investigated v0.3.0 writers, including
// process interruption. Only the explicitly labeled coarse-time cases have
// modified mtimes. See testdata/released-queues.json and docs/compatibility.md.
func TestReleasedQueueFiles(t *testing.T) {
	type savedFile struct {
		Data    []byte `json:"data"`
		MtimeNS int64  `json:"mtime_ns"`
	}
	var fixtures []struct {
		Name        string
		Source      string
		Size        uint64
		Pages       uint64
		Kind        QueueType
		Sync        bool
		Group       bool
		ExpectError bool `json:"expect_error"`
		Files       map[string]savedFile
		Expected    map[string][]int
	}
	data, err := os.ReadFile("testdata/released-queues.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			beforeOpen, beforeCreate := fOpenFlag, fCreateFlag
			defer func() { fOpenFlag, fCreateFlag = beforeOpen, beforeCreate }()
			if fixture.Sync {
				SetFSync()
			}
			dir := t.TempDir()
			for name, file := range fixture.Files {
				path := filepath.Join(dir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, file.Data, 0644); err != nil {
					t.Fatal(err)
				}
				stamp := time.Unix(0, file.MtimeNS)
				if err := os.Chtimes(path, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() map[string]savedFile {
				files := make(map[string]savedFile)
				err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
					if err != nil || entry.IsDir() {
						return err
					}
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					info, err := entry.Info()
					if err != nil {
						return err
					}
					name, err := filepath.Rel(dir, path)
					files[name] = savedFile{data, info.ModTime().UnixNano()}
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
				return files
			}
			before := snapshot()
			var q interface {
				Dequeue() (*Message[int], error)
				UpdateIndex(*Message[int]) error
				WaitInitialize()
				WaitInitializeWithError() error
				CloseQueue() error
				CloseIndex() error
			}
			opts := []Option{WithFileDir(dir), WithQueueSize(fixture.Size), WithMaxPage(fixture.Pages), WithQueueType(fixture.Kind)}
			var err error
			if fixture.Group {
				q, err = NewGroupQueue[int]("fixtures", opts...)
			} else {
				q, err = NewQueue[int]("fixture", opts...)
			}
			if err != nil {
				t.Fatalf("%s: %v", fixture.Source, err)
			}
			t.Cleanup(func() { q.CloseQueue(); q.CloseIndex() })
			type result struct {
				values map[string][]int
				err    error
			}
			done := make(chan result, 1)
			go func() {
				values := make(map[string][]int)
				for {
					m, err := q.Dequeue()
					if err != nil {
						if IsErrQueueClose(err) {
							err = q.CloseIndex()
						} else {
							q.CloseIndex()
						}
						done <- result{values, err}
						return
					}
					values[m.Name()] = append(values[m.Name()], *m.Item())
					if err := q.UpdateIndex(m); err != nil {
						done <- result{values, err}
						return
					}
				}
			}()
			initErr := q.WaitInitializeWithError()
			if fixture.ExpectError && !isRecoveryFailure(initErr) || !fixture.ExpectError && initErr != nil {
				t.Fatalf("released startup result: %v", initErr)
			}
			if err := q.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if fixture.ExpectError {
				if !isRecoveryFailure(got.err) || len(got.values) != 0 {
					t.Fatalf("ambiguous released files: values=%v, error=%v", got.values, got.err)
				}
				if !reflect.DeepEqual(snapshot(), before) {
					t.Fatal("failed recovery changed released files")
				}
				return
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			want := make(map[string][]int)
			for name, values := range fixture.Expected {
				if len(values) != 0 {
					want[name] = values
				}
			}
			if !reflect.DeepEqual(got.values, want) {
				t.Fatalf("released values: got %v, want %v", got.values, want)
			}
			for name, file := range before {
				if strings.HasPrefix(filepath.Base(name), "queue.") {
					data, err := os.ReadFile(filepath.Join(dir, name))
					if err != nil || !bytes.Equal(data, file.Data) {
						t.Fatalf("recovery changed released payload %s: %v", name, err)
					}
				}
			}
		})
	}
}

func TestPersistedIndexRemainsCircular(t *testing.T) {
	for _, kind := range []QueueType{SPSC, MPSC} {
		t.Run(string(kind), func(t *testing.T) {
			q, err := NewQueue[int]("q", WithFileDir(t.TempDir()), WithQueueSize(2), WithMaxPage(3), WithQueueType(kind))
			if err != nil {
				t.Fatal(err)
			}
			q.WaitInitialize()
			defer func() { q.CloseQueue(); q.CloseIndex() }()
			for i := 0; i < 14; i++ {
				value := i
				if err := q.Enqueue(&value); err != nil {
					t.Fatal(err)
				}
				message, err := q.Dequeue()
				if err != nil {
					t.Fatal(err)
				}
				want := uint64(i % 6)
				if message.Index() != want {
					t.Fatalf("public index: got %d, want %d", message.Index(), want)
				}
				// Persist progress at byte zero regardless of the current offset.
				if _, err := q.indexFile.Seek(23, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				if err := q.UpdateIndex(message); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(q.fileDir, "index"))
				if err != nil {
					t.Fatal(err)
				}
				if len(data) != 8 || binary.LittleEndian.Uint64(data) != want {
					t.Fatalf("persisted index at %d: %x", i, data)
				}
			}
		})
	}
}

func TestMessageIdentityAndFileCompatibility(t *testing.T) {
	var zero Message[int]
	if zero.Name() != "" || zero.Index() != 0 || zero.Item() != nil {
		t.Fatal("zero Message behavior changed")
	}
	for _, syncWrites := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync=%t", syncWrites), func(t *testing.T) {
			beforeOpen, beforeCreate := fOpenFlag, fCreateFlag
			defer func() { fOpenFlag, fCreateFlag = beforeOpen, beforeCreate }()
			if syncWrites {
				SetFSync()
			}
			dir := t.TempDir()
			opts := []Option{WithFileDir(dir), WithQueueSize(2), WithMaxPage(3)}
			q, err := NewQueue[int]("identity", opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if q != nil {
					q.CloseQueue()
					q.CloseIndex()
				}
			})
			if err := q.waitInitialize(); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(dir, "index")); err != nil || len(data) != 0 {
				t.Fatalf("initial index: %x, %v", data, err)
			}
			first, second := 7, 8
			if err := q.Enqueue(&first); err != nil {
				t.Fatal(err)
			}
			m, err := q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if m.Name() != "identity" || m.Index() != 0 || *m.Item() != first {
				t.Fatalf("first message: %+v", m)
			}
			if err := q.UpdateIndex(m); err != nil {
				t.Fatal(err)
			}
			if err := q.Enqueue(&second); err != nil {
				t.Fatal(err)
			}
			if err := q.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			if err := q.CloseIndex(); err != nil {
				t.Fatal(err)
			}
			if m.Name() != "identity" {
				t.Fatal("name changed after close")
			}
			q, err = NewQueue[int]("identity", opts...)
			if err != nil {
				t.Fatal(err)
			}
			m, err = q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if err := q.waitInitialize(); err != nil {
				t.Fatal(err)
			}
			if m.Name() != "identity" || m.Index() != 1 || *m.Item() != second {
				t.Fatalf("recovered message: %+v", m)
			}
			if err := q.UpdateIndex(m); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "index"))
			if err != nil || !bytes.Equal(data, []byte{1, 0, 0, 0, 0, 0, 0, 0}) {
				t.Fatalf("index bytes: %x, %v", data, err)
			}
			data, err = os.ReadFile(filepath.Join(dir, "queue.0"))
			if err != nil || string(data) != "[7]\n[8]\n" {
				t.Fatalf("page bytes: %q, %v", data, err)
			}
		})
	}
}

func TestReadyOperationsObserveFailure(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprintf("group=%t", grouped), func(t *testing.T) {
			dir := t.TempDir()
			var q *Queue[int]
			var group *GroupQueue[int]
			var err error
			value := 1
			if grouped {
				group, err = NewGroupQueue[int]("g", WithFileDir(dir))
				if err != nil {
					t.Fatal(err)
				}
				group.WaitInitialize()
				if err := group.Enqueue("a", &value); err != nil {
					t.Fatal(err)
				}
				if err := group.Enqueue("b", &value); err != nil {
					t.Fatal(err)
				}
				q, err = group.getQueue("b")
			} else {
				q, err = NewQueue[int]("q", WithFileDir(dir))
				if err == nil {
					q.WaitInitialize()
					err = q.Enqueue(&value)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if grouped {
					group.CloseQueue()
					group.CloseIndex()
				} else {
					q.CloseQueue()
					q.CloseIndex()
				}
			}()
			before := diskSnapshot(t, q.fileDir)
			injected := fmt.Errorf("injected failure")
			if grouped {
				a, err := group.getQueue("a")
				if err != nil {
					t.Fatal(err)
				}
				a.fail(injected)
			} else {
				q.fail(injected)
			}
			if _, err := q.Dequeue(); err != injected {
				t.Fatalf("ready dequeue: %v", err)
			}
			if err := q.Enqueue(&value); err != injected {
				t.Fatalf("ready enqueue: %v", err)
			}
			if err := q.publish(&Message[int]{item: &value, owner: q}); err != injected {
				t.Fatalf("ready publish: %v", err)
			}
			if q.Length() != 1 {
				t.Fatalf("failed operation consumed or published data: %d", q.Length())
			}
			if after := diskSnapshot(t, q.fileDir); !reflect.DeepEqual(before, after) {
				t.Fatal("failed operation changed stored data")
			}
		})
	}
}

func TestBulkWaitersObserveFailure(t *testing.T) {
	for _, mode := range []string{"queue", "group-child", "group"} {
		for _, callback := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/callback=%t", mode, callback), func(t *testing.T) {
				var q, failing *Queue[int]
				var group *GroupQueue[int]
				var receiver interface {
					BulkDequeue(uint64, time.Duration) ([]*Message[int], error)
					FuncAfterBulkDequeue(uint64, time.Duration, func([]*int) error) (int, error)
				}
				value := 7
				if mode == "queue" {
					var err error
					q, err = NewQueue[int]("q", WithFileDir(t.TempDir()), WithQueueSize(4))
					if err != nil {
						t.Fatal(err)
					}
					q.WaitInitialize()
					failing, receiver = q, q
					if err := q.Enqueue(&value); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { q.CloseQueue(); q.CloseIndex() })
				} else {
					var err error
					group, err = NewGroupQueue[int]("g", WithFileDir(t.TempDir()), WithQueueSize(4))
					if err != nil {
						t.Fatal(err)
					}
					group.WaitInitialize()
					t.Cleanup(func() { group.CloseQueue(); group.CloseIndex() })
					if err := group.addQueue("source"); err != nil {
						t.Fatal(err)
					}
					if err := group.Enqueue("target", &value); err != nil {
						t.Fatal(err)
					}
					q, err = group.getQueue("target")
					if err != nil {
						t.Fatal(err)
					}
					failing, err = group.getQueue("source")
					if err != nil {
						t.Fatal(err)
					}
					receiver = q
					if mode == "group" {
						receiver = group
					}
				}
				before := diskSnapshot(t, q.fileDir)
				type result struct {
					values []int
					count  int
					err    error
				}
				done := make(chan result, 1)
				called := false
				go func() {
					if callback {
						n, err := receiver.FuncAfterBulkDequeue(3, 10*time.Second, func([]*int) error { called = true; return nil })
						done <- result{count: n, err: err}
						return
					}
					messages, err := receiver.BulkDequeue(3, 10*time.Second)
					var values []int
					for _, m := range messages {
						values = append(values, *m.Item())
					}
					done <- result{values: values, err: err}
				}()
				deadline := time.Now().Add(3 * time.Second)
				for q.Length() != 0 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				if q.Length() != 0 {
					t.Fatal("bulk call did not consume its first item")
				}
				injected := fmt.Errorf("injected bulk failure")
				failing.fail(injected)
				select {
				case got := <-done:
					if got.err != injected {
						t.Fatalf("bulk failure: %v", got.err)
					}
					if callback {
						if called || got.count != 0 {
							t.Fatal("failed bulk call processed or acknowledged items")
						}
					} else if !reflect.DeepEqual(got.values, []int{7}) {
						t.Fatalf("partial bulk result: %v", got.values)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("bulk waiter was not released by failure")
				}
				if !reflect.DeepEqual(diskSnapshot(t, q.fileDir), before) {
					t.Fatal("failed bulk call changed data or persisted progress")
				}
			})
		}
	}
}

func diskSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]string)
	for _, entry := range entries {
		b, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = string(b)
	}
	return files
}

func drainCompatibilityQueue(t *testing.T, q *Queue[int]) ([]int, error) {
	t.Helper()
	type result struct {
		values []int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		var values []int
		for {
			m, err := q.Dequeue()
			if err != nil {
				if IsErrQueueClose(err) {
					err = nil
				}
				if e := q.CloseIndex(); err == nil {
					err = e
				}
				done <- result{values, err}
				return
			}
			values = append(values, *m.Item())
			if err := q.UpdateIndex(m); err != nil {
				done <- result{values, err}
				return
			}
		}
	}()
	q.WaitInitialize()
	if err := q.CloseQueue(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		return r.values, r.err
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
		return nil, nil
	}
}

func TestStorageContractGolden(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "bulk"}[bulk], func(t *testing.T) {
			dir := t.TempDir()
			q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2), WithMaxPage(3))
			if err != nil {
				t.Fatal(err)
			}
			q.WaitInitialize()
			if got := diskSnapshot(t, dir); !reflect.DeepEqual(got, map[string]string{"index": "", "queue.0": ""}) {
				t.Fatalf("initial files: %q", got)
			}
			for start := 0; start < 5; {
				n := 1
				if bulk && start < 4 {
					n = 2
				}
				items := make([]*int, n)
				for i := range items {
					v := start + i
					items[i] = &v
				}
				if bulk {
					err = q.BulkEnqueue(items)
				} else {
					err = q.Enqueue(items[0])
				}
				if err != nil {
					t.Fatal(err)
				}
				for range items {
					m, e := q.Dequeue()
					if e != nil {
						t.Fatal(e)
					}
					if e = q.UpdateIndex(m); e != nil {
						t.Fatal(e)
					}
				}
				start += n
			}
			q.CloseQueue()
			q.CloseIndex()
			var index [8]byte
			binary.LittleEndian.PutUint64(index[:], 4)
			want := map[string]string{"index": string(index[:]), "queue.0": "[0]\n[1]\n", "queue.1": "[2]\n[3]\n", "queue.2": "[4]\n"}
			if bulk {
				want["queue.0"] = "[0,1]\n"
				want["queue.1"] = "[2,3]\n"
			}
			if got := diskSnapshot(t, dir); !reflect.DeepEqual(got, want) {
				t.Fatalf("file contract changed: got %q want %q", got, want)
			}
		})
	}
}

func legacyFixture(t *testing.T, dir string, files map[string]string, index uint64, stamps map[string]int64) {
	t.Helper()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], index)
	if err := os.WriteFile(filepath.Join(dir, "index"), b[:], 0644); err != nil {
		t.Fatal(err)
	}
	for name, stamp := range stamps {
		when := time.Unix(stamp, 0)
		if err := os.Chtimes(filepath.Join(dir, name), when, when); err != nil {
			t.Fatal(err)
		}
	}
}

// Give a known test history distinguishable timestamps. Real filesystems can
// give successive writes equal mtimes; those states are tested as ambiguous,
// rather than asserting that the legacy format can always recover them.
func stampKnownHistory(t *testing.T, dir string, size, pages, completed, pending uint64) {
	t.Helper()
	head := completed + pending
	if head == 0 {
		return
	}
	lastPage := (head - 1) / size
	firstPage := uint64(0)
	if lastPage >= pages {
		firstPage = lastPage - pages + 1
	}
	for page := firstPage; page <= lastPage; page++ {
		end := (page + 1) * size
		if end > head {
			end = head
		}
		stamp := time.Unix(int64(100+end*2), 0)
		if err := os.Chtimes(filepath.Join(dir, "queue."+strconv.FormatUint(page%pages, 10)), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if completed > 0 {
		stamp := time.Unix(int64(100+completed*2+1), 0)
		if err := os.Chtimes(filepath.Join(dir, "index"), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLegacyRecoveryContract(t *testing.T) {
	for _, tc := range []struct {
		name   string
		index  uint64
		files  map[string]string
		stamps map[string]int64
		want   []int
	}{
		{"control", 2, map[string]string{"queue.0": "[0]\n[1]\n", "queue.1": "[2]\n[3]\n"}, map[string]int64{"queue.0": 10, "queue.1": 20, "index": 15}, []int{3}},
		{"wrap-partial", 3, map[string]string{"queue.0": "[4]\n", "queue.1": "[2]\n[3]\n"}, map[string]int64{"queue.0": 30, "queue.1": 20, "index": 25}, []int{4}},
		{"all-completed", 3, map[string]string{"queue.0": "[0]\n[1]\n", "queue.1": "[2]\n[3]\n"}, map[string]int64{"queue.0": 10, "queue.1": 20, "index": 30}, nil},
		{"wrap-full", 3, map[string]string{"queue.0": "[4]\n[5]\n", "queue.1": "[2]\n[3]\n"}, map[string]int64{"queue.0": 30, "queue.1": 20, "index": 25}, []int{4, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			legacyFixture(t, dir, tc.files, tc.index, tc.stamps)
			q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2))
			if err != nil {
				t.Fatal(err)
			}
			got, err := drainCompatibilityQueue(t, q)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("recovered %v want %v", got, tc.want)
			}
			names := make([]string, 0)
			for name := range diskSnapshot(t, dir) {
				names = append(names, name)
			}
			sort.Strings(names)
			if !reflect.DeepEqual(names, []string{"index", "queue.0", "queue.1"}) {
				t.Fatalf("unexpected files %v", names)
			}
		})
	}
}

// Recovery boundaries and production configuration.

func collectRecovered(t *testing.T, q *Queue[int]) []int {
	t.Helper()
	var values []int
	done := make(chan error, 1)
	go func() {
		for {
			m, err := q.Dequeue()
			if err != nil {
				closeErr := q.CloseIndex()
				if IsErrQueueClose(err) {
					err = closeErr
				}
				done <- err
				return
			}
			values = append(values, *m.Item())
			if err := q.UpdateIndex(m); err != nil {
				done <- err
				return
			}
		}
	}()
	if err := q.waitInitialize(); err != nil {
		q.CloseQueue()
		<-done
		t.Fatal(err)
	}
	if err := q.CloseQueue(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not terminate")
	}
	return values
}

func TestRecoveryCircularBoundary(t *testing.T) {
	for _, cfg := range []struct{ size, pages uint64 }{{1, 2}, {2, 2}, {3, 3}, {2048, 3}} {
		for _, kind := range []QueueType{SPSC, MPSC} {
			for _, completed := range []uint64{cfg.size*cfg.pages - 1, cfg.size * cfg.pages, 2 * cfg.size * cfg.pages} {
				for _, pending := range []int{0, 1} {
					t.Run(fmt.Sprintf("%d-%d-%s-%d-%d", cfg.size, cfg.pages, kind, completed, pending), func(t *testing.T) {
						dir := t.TempDir()
						opts := []Option{WithFileDir(dir), WithQueueSize(cfg.size), WithMaxPage(cfg.pages), WithQueueType(kind)}
						q, err := NewQueue[int]("q", opts...)
						if err != nil {
							t.Fatal(err)
						}
						q.WaitInitialize()
						for i := uint64(0); i < completed; i++ {
							v := int(i)
							if err := q.Enqueue(&v); err != nil {
								t.Fatal(err)
							}
							m, err := q.Dequeue()
							if err != nil {
								t.Fatal(err)
							}
							if err := q.UpdateIndex(m); err != nil {
								t.Fatal(err)
							}
						}
						for i := 0; i < pending; i++ {
							v := int(completed) + i
							if err := q.Enqueue(&v); err != nil {
								t.Fatal(err)
							}
						}
						if err := q.CloseQueue(); err != nil {
							t.Fatal(err)
						}
						if err := q.CloseIndex(); err != nil {
							t.Fatal(err)
						}
						stampKnownHistory(t, dir, cfg.size, cfg.pages, completed, uint64(pending))
						q, err = NewQueue[int]("q", opts...)
						if err != nil {
							t.Fatal(err)
						}
						got := collectRecovered(t, q)
						var want []int
						if pending != 0 {
							want = []int{int(completed)}
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("got %v, want %v", got, want)
						}
						if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("queue.%d", cfg.pages))); !os.IsNotExist(err) {
							t.Fatal("out-of-range page exists")
						}
						// Append after a second restart, including an empty circular boundary.
						q, err = NewQueue[int]("q", opts...)
						if err != nil {
							t.Fatal(err)
						}
						q.WaitInitialize()
						v := 999999
						if err := q.Enqueue(&v); err != nil {
							t.Fatal(err)
						}
						got = collectRecovered(t, q)
						if !reflect.DeepEqual(got, []int{v}) {
							t.Fatalf("append got %v", got)
						}
					})
				}
			}
		}
	}
}

func TestInitializationFailureIsObservable(t *testing.T) {
	const payload = "DUMMY_FFQ_PANIC_ONLY"
	dir := t.TempDir()
	original := []byte(`[{"fixture":"` + payload + `"`)
	if err := os.WriteFile(filepath.Join(dir, "queue.0"), original, 0600); err != nil {
		t.Fatal(err)
	}
	q, err := NewQueue[int]("q", WithFileDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer q.CloseIndex()
	defer q.CloseQueue()
	err = q.waitInitialize()
	if !isRecoveryFailure(err) || strings.Contains(err.Error(), payload) {
		t.Fatalf("unsafe/unexpected error: %v", err)
	}
	if _, err := q.Dequeue(); !isRecoveryFailure(err) {
		t.Fatalf("dequeue error: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "queue.0"))
	if !reflect.DeepEqual(got, original) {
		t.Fatal("damaged data was modified")
	}
}

func TestInvalidIndexPreserved(t *testing.T) {
	for _, invalid := range [][]byte{{1, 0, 0}, binary.LittleEndian.AppendUint64(nil, 10)} {
		dir := t.TempDir()
		path := filepath.Join(dir, "index")
		if err := os.WriteFile(path, invalid, 0600); err != nil {
			t.Fatal(err)
		}
		q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(4), WithMaxPage(2))
		if err == nil {
			q.WaitInitialize()
			q.CloseQueue()
			q.CloseIndex()
			t.Fatal("invalid index accepted")
		}
		got, _ := os.ReadFile(path)
		if !reflect.DeepEqual(got, invalid) {
			t.Fatal("invalid index was modified")
		}
	}
}

func TestBulkRecoveryProductionConfiguration(t *testing.T) {
	for _, kind := range []QueueType{SPSC, MPSC} {
		for _, syncWrites := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sync=%t", kind, syncWrites), func(t *testing.T) {
				beforeOpen, beforeCreate := fOpenFlag, fCreateFlag
				defer func() { fOpenFlag, fCreateFlag = beforeOpen, beforeCreate }()
				if syncWrites {
					SetFSync()
				}
				dir := t.TempDir()
				opts := []Option{WithFileDir(dir), WithQueueSize(2048), WithMaxPage(3), WithQueueType(kind)}
				q, err := NewQueue[int]("q", opts...)
				if err != nil {
					t.Fatal(err)
				}
				const completed = 6144
				ack := make(chan int, 1)
				held, stop, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				go func() {
					processed := 0
					for {
						messages, err := q.BulkDequeue(50, 10*time.Millisecond)
						if err != nil {
							done <- err
							return
						}
						if *messages[0].Item() >= completed {
							close(held)
							<-stop
							done <- q.CloseIndex()
							return // model interruption before this batch's update
						}
						for _, m := range messages {
							if *m.Item() != processed {
								done <- fmt.Errorf("order: got %d, want %d", *m.Item(), processed)
								return
							}
							processed++
						}
						if err := q.UpdateIndex(messages[len(messages)-1]); err != nil {
							done <- err
							return
						}
						ack <- processed
					}
				}()
				if err := q.WaitInitializeWithError(); err != nil {
					t.Fatal(err)
				}
				for start := 0; start < completed; {
					count := 50
					if completed-start < count {
						count = completed - start
					}
					items := make([]*int, count)
					for i := range items {
						v := start + i
						items[i] = &v
					}
					if err := q.BulkEnqueue(items); err != nil {
						t.Fatal(err)
					}
					start += count
					for n := <-ack; n < start; n = <-ack {
					}
				}
				items := make([]*int, 50)
				for i := range items {
					v := completed + i
					items[i] = &v
				}
				if err := q.BulkEnqueue(items); err != nil {
					t.Fatal(err)
				}
				<-held
				if err := q.CloseQueue(); err != nil {
					t.Fatal(err)
				}
				close(stop)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				// A partial newest page identifies the pending range even when timestamps coincide.
				for _, name := range []string{"queue.0", "queue.1", "queue.2", "index"} {
					stamp := time.Unix(1, 0)
					if err := os.Chtimes(filepath.Join(dir, name), stamp, stamp); err != nil {
						t.Fatal(err)
					}
				}
				q, err = NewQueue[int]("q", opts...)
				if err != nil {
					t.Fatal(err)
				}
				got := collectRecovered(t, q)
				want := make([]int, 50)
				for i := range want {
					want[i] = completed + i
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("restored %d items, want %d", len(got), len(want))
				}
			})
		}
	}
}

func isRecoveryFailure(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "initialize queue:")
}
func isIOFailure(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "queue I/O failed")
}
func isProgressFailure(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "queue progress:")
}

// Page protection and recovery failure handling.

func TestAmbiguousLegacyRecoveryPreservesFiles(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{"queue.0": []byte("[0]\n[1]\n"), "queue.1": []byte("[2]\n[3]\n"), "index": binary.LittleEndian.AppendUint64(nil, 3)}
	for name, data := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(1, 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	before := diskSnapshot(t, dir)
	q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.waitInitialize(); !isRecoveryFailure(err) {
		t.Fatalf("error: %v", err)
	}
	q.CloseQueue()
	q.CloseIndex()
	if after := diskSnapshot(t, dir); !reflect.DeepEqual(after, before) {
		t.Fatalf("ambiguous storage changed: %q", after)
	}
	for name, want := range files {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("modified %s", name)
		}
	}
}

func waitForPageProtection(t *testing.T) {
	t.Helper()
	waitForQueueFunction(t, "waitPage")
}

func waitForQueueFunction(t *testing.T, function string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	buf := make([]byte, 65536)
	for {
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), "."+function+"(") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("producer did not wait in %s", function)
		}
		runtime.Gosched()
	}
}

func TestUnacknowledgedPageProtectedBeforeWrite(t *testing.T) {
	for _, kind := range []QueueType{SPSC, MPSC} {
		t.Run(string(kind), func(t *testing.T) {
			dir := t.TempDir()
			opts := []Option{WithFileDir(dir), WithQueueSize(2), WithMaxPage(2), WithQueueType(kind)}
			q, err := NewQueue[int]("q", opts...)
			if err != nil {
				t.Fatal(err)
			}
			q.WaitInitialize()
			v := 0
			if err := q.Enqueue(&v); err != nil {
				t.Fatal(err)
			}
			m, _ := q.Dequeue()
			if err := q.UpdateIndex(m); err != nil {
				t.Fatal(err)
			}
			v = 1
			if err := q.Enqueue(&v); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Dequeue(); err != nil {
				t.Fatal(err)
			} // processing, not acknowledged
			a, b := 2, 3
			if err := q.BulkEnqueue([]*int{&a, &b}); err != nil {
				t.Fatal(err)
			}
			pending := make(chan error, 1)
			go func() { v := 4; pending <- q.Enqueue(&v) }()
			waitForPageProtection(t)
			before, err := os.ReadFile(filepath.Join(dir, "queue.0"))
			if err != nil || string(before) != "[0]\n[1]\n" {
				t.Fatalf("unfinished page was changed: %s", before)
			}
			// Closing while this specific producer waits on progress must release
			// it without truncation. Join it before touching the index.
			if err := q.CloseQueue(); err != nil {
				t.Fatal(err)
			}
			if err := <-pending; !IsErrQueueClose(err) {
				t.Fatalf("enqueue: %v", err)
			}
			q.CloseIndex()
			stampKnownHistory(t, dir, 2, 2, 1, 3)
			q, err = NewQueue[int]("q", opts...)
			if err != nil {
				t.Fatal(err)
			}
			if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{1, 2, 3}) {
				t.Fatalf("got %v", got)
			}
		})
	}
}

func TestRotationRetryAfterOpenFailure(t *testing.T) {
	dir := t.TempDir()
	q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2))
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	for i := 0; i < 2; i++ {
		v := i
		if err := q.Enqueue(&v); err != nil {
			t.Fatal(err)
		}
		m, _ := q.Dequeue()
		if err := q.UpdateIndex(m); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "queue.1")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	v := 2
	if err := q.Enqueue(&v); err == nil {
		t.Fatal("injected open failure ignored")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(&v); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("got %v", got)
	}
}

func TestInitializationFailureOutput(t *testing.T) {
	const marker = "DUMMY_FFQ_PANIC_ONLY"
	if dir := os.Getenv("FFQ_INIT_FAILURE_HELPER"); dir != "" {
		q, err := NewQueue[int]("q", WithFileDir(dir), WithDecoder(func([]byte, any) error { return fmt.Errorf("decoder error: %s", marker) }))
		if err != nil {
			t.Fatal(err)
		}
		err = q.waitInitialize()
		if !isRecoveryFailure(err) {
			t.Fatalf("error: %v", err)
		}
		fmt.Println(err)
		q.CloseQueue()
		q.CloseIndex()
		return
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "queue.0"), []byte("[1]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestInitializationFailureOutput$")
	cmd.Env = append(os.Environ(), "FFQ_INIT_FAILURE_HELPER="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil || bytes.Contains(out, []byte(marker)) {
		t.Fatalf("unsafe initialization output: %v %s", err, out)
	}
}

func TestAppendAfterLegacyRecordWithoutNewline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "queue.0"), []byte("[1]"), 0600); err != nil {
		t.Fatal(err)
	}
	opts := []Option{WithFileDir(dir), WithQueueSize(4)}
	q, err := NewQueue[int]("q", opts...)
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{1}) {
		t.Fatal(got)
	}
	q, err = NewQueue[int]("q", opts...)
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	v := 2
	if err := q.Enqueue(&v); err != nil {
		t.Fatal(err)
	}
	q.CloseQueue()
	q.CloseIndex()
	q, err = NewQueue[int]("q", opts...)
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{2}) {
		t.Fatal(got)
	}
}

func TestPageProtectionReleasedByProgress(t *testing.T) {
	q, err := NewQueue[int]("q", WithFileDir(t.TempDir()), WithQueueSize(2))
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	a, b := 0, 1
	if err := q.BulkEnqueue([]*int{&a, &b}); err != nil {
		t.Fatal(err)
	}
	batch, err := q.BulkDequeue(2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c, d := 2, 3
	if err := q.BulkEnqueue([]*int{&c, &d}); err != nil {
		t.Fatal(err)
	}
	producer := make(chan error, 1)
	go func() { v := 4; producer <- q.Enqueue(&v) }()
	waitForPageProtection(t)
	if err := q.UpdateIndex(batch[len(batch)-1]); err != nil {
		t.Fatal(err)
	}
	m, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.UpdateIndex(m); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-producer:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("progress did not release producer")
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{3, 4}) {
		t.Fatal(got)
	}
}

func TestIndexFailureReleasesPageWaiter(t *testing.T) {
	dir := t.TempDir()
	q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2))
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	for i := 0; i < 2; i++ {
		v := i
		if err := q.Enqueue(&v); err != nil {
			t.Fatal(err)
		}
		m, _ := q.Dequeue()
		if i == 0 {
			if err := q.UpdateIndex(m); err != nil {
				t.Fatal(err)
			}
		} else {
			a, b := 2, 3
			if err := q.BulkEnqueue([]*int{&a, &b}); err != nil {
				t.Fatal(err)
			}
			producer := make(chan error, 1)
			go func() { v := 4; producer <- q.Enqueue(&v) }()
			waitForPageProtection(t)
			if err := q.indexFile.Close(); err != nil {
				t.Fatal(err)
			} // inject index I/O failure
			if err := q.UpdateIndex(m); !isIOFailure(err) {
				t.Fatalf("index error: %v", err)
			}
			select {
			case err := <-producer:
				if !isIOFailure(err) {
					t.Fatalf("producer: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("failed progress did not release producer")
			}
		}
	}
	q.CloseQueue()
	q.CloseIndex()
	stampKnownHistory(t, dir, 2, 2, 1, 3)
	q, err = NewQueue[int]("q", WithFileDir(dir), WithQueueSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatal(got)
	}
}

func TestProgressRejectsForeignAndBackwardMessages(t *testing.T) {
	q, err := NewQueue[int]("same", WithFileDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewQueue[int]("same", WithFileDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	other.WaitInitialize()
	defer q.CloseIndex()
	defer q.CloseQueue()
	defer other.CloseIndex()
	defer other.CloseQueue()
	a, b := 0, 1
	if err := q.BulkEnqueue([]*int{&a, &b}); err != nil {
		t.Fatal(err)
	}
	first, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	last, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := other.UpdateIndex(first); !isProgressFailure(err) {
		t.Fatalf("foreign message: %v", err)
	}
	if err := q.UpdateIndex(last); err != nil {
		t.Fatal(err)
	}
	if err := q.UpdateIndex(first); !isProgressFailure(err) {
		t.Fatalf("backward progress: %v", err)
	}
	if err := q.UpdateIndex(last); err != nil {
		t.Fatal("idempotent progress:", err)
	}
}

func TestDecoderPanicIsAnInitializationError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "queue.0"), []byte("[1]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	const marker = "DUMMY_FFQ_PANIC_ONLY"
	q, err := NewQueue[int]("q", WithFileDir(dir), WithDecoder(func([]byte, any) error { panic(marker) }))
	if err != nil {
		t.Fatal(err)
	}
	defer q.CloseIndex()
	defer q.CloseQueue()
	err = q.waitInitialize()
	if !isRecoveryFailure(err) || strings.Contains(err.Error(), marker) {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestReadOnlyLegacyWriterDoesNotChangeIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.0")
	if err := os.WriteFile(path, []byte("[0]\n[1]\n"), 0400); err != nil {
		t.Fatal(err)
	}
	index := binary.LittleEndian.AppendUint64(nil, 0)
	if err := os.WriteFile(filepath.Join(dir, "index"), index, 0600); err != nil {
		t.Fatal(err)
	}
	q, err := NewQueue[int]("q", WithFileDir(dir), WithQueueSize(4))
	if err != nil {
		t.Fatal(err)
	}
	defer q.CloseIndex()
	defer q.CloseQueue()
	if err := q.waitInitialize(); !isRecoveryFailure(err) {
		t.Fatalf("error: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "index"))
	if err != nil || !bytes.Equal(got, index) {
		t.Fatal("initialization failure changed legacy index")
	}
}

func TestEmptyLegacyPagesDoNotInventProgress(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("queue.%d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	opts := []Option{WithFileDir(dir), WithQueueSize(2), WithMaxPage(3)}
	q, err := NewQueue[int]("q", opts...)
	if err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); len(got) != 0 {
		t.Fatal(got)
	}
	q, err = NewQueue[int]("q", opts...)
	if err != nil {
		t.Fatal(err)
	}
	q.WaitInitialize()
	v := 1
	if err := q.Enqueue(&v); err != nil {
		t.Fatal(err)
	}
	if got := collectRecovered(t, q); !reflect.DeepEqual(got, []int{1}) {
		t.Fatal(got)
	}
}
