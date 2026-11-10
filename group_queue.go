// Package ffq provides a File-based FIFO Queue implementation that supports generic types.
package ffq

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// GroupQueue represents a group of queues with common configurations like size, encoder, and decoder.
// It manages multiple named queues and supports operations like enqueue, dequeue, and bulk enqueue/dequeue.
type GroupQueue[T any] struct {
	size            uint64                         // The maximum number of items in one file page.
	maxPage         uint64                         // The number of file pages for file rotation.
	queueCount      uint32                         // The current count of queues in the group.
	closeCount      uint32                         // The number of queues that have been closed.
	activeQueue     uint32                         // The index of the currently active queue.
	groupSize       int                            // The maximum number of individual queues allowed in the group.
	name            string                         // The name of the group queue.
	fileDir         string                         // The directory where queue files are stored.
	queues          []*Queue[T]                    // The collection of individual Queue instances.
	nameIndices     sync.Map                       // Maps queue names to their index within the group.
	queueType       QueueType                      // The operating mode (SPSC or MPSC) for the queues.
	isClose         atomic.Bool                    // Flag indicating if the group queue is closed.
	encoder         func(v any) ([]byte, error)    // Encoder function for serializing data.
	decoder         func(data []byte, v any) error // Decoder function for deserializing data.
	initializeBlock chan struct{}                  // Channel used to block until initialization is complete.
	enqueueSig      chan struct{}                  // Signal channel to notify enqueue operations.
	closeSig        chan struct{}                  // Signal channel to notify that the group queue is closed.
	failureSig      chan struct{}
	failure         atomic.Pointer[queueFailure]
	addMu           sync.Mutex
	mu              sync.RWMutex // Mutex to synchronize access to the group's queues.
}

// NewGroupQueue initializes a new GroupQueue with the given name and optional settings.
// It sets up the underlying queues, file directory, encoder, and decoder to manage the queue data.
//
// Parameters:
//   - name: The name of the group queue.
//   - opts: Optional settings for the group queue (e.g., WithQueueSize, WithMaxPage, WithQueueType, WithFileDir, WithGroupSize, WithEncoder, WithDecoder).
//
// Returns:
//   - *GroupQueue[T]: A pointer to the newly created GroupQueue.
//   - error: An error if any issues occur during initialization (e.g., failure to create the file directory).
//
// Example:
//
//	// Create a group queue for Data items with a queue size of 1024 and 2 pages.
//	gq, err := ffq.NewGroupQueue[Data]("myGroupQueue",
//	     ffq.WithQueueSize(1024),
//	     ffq.WithMaxPage(2),
//	     ffq.WithGroupSize(10),
//	     ffq.WithQueueType(ffq.SPSC))
//	if err != nil {
//	    log.Fatalf("Failed to create group queue: %v", err)
//	}
//	// Start the consumer first, then check initialization before starting producers.
//	if err := gq.WaitInitializeWithError(); err != nil {
//	    log.Fatal(err)
//	}
func NewGroupQueue[T any](name string, opts ...Option) (*GroupQueue[T], error) {
	var err error

	// check options and set default settings
	var options options
	for _, opt := range opts {
		err := opt(&options)
		if err != nil {
			return nil, err
		}
	}

	var fileDir = "/tmp/ffq"
	if options.fileDir != nil {
		fileDir = *options.fileDir
	}
	err = createQueueDir(fileDir)
	if err != nil {
		return nil, err
	}

	var size uint64 = 1024
	if options.size != nil {
		size = *options.size
	}

	var maxPage uint64 = 2
	if options.maxPage != nil {
		maxPage = *options.maxPage
	}

	var queueType QueueType = SPSC
	if options.queueType != nil {
		queueType = *options.queueType
	}

	var groupSize int = 10
	if options.groupSize != nil {
		groupSize = *options.groupSize
	}

	var encoder func(v any) ([]byte, error) = json.Marshal
	if options.encoder != nil {
		encoder = *options.encoder
	}

	var decoder func(data []byte, v any) error = json.Unmarshal
	if options.decoder != nil {
		decoder = *options.decoder
	}

	if groupSize < 1 || size > uint64(int(^uint(0)>>1)) || maxPage > uint64(int(^uint(0)>>1)) || size > ^uint64(0)/maxPage || (queueType != SPSC && queueType != MPSC) || encoder == nil || decoder == nil {
		return nil, fmt.Errorf("%w: invalid group configuration", ErrQueueOption)
	}
	queues := make([]*Queue[T], groupSize)
	enqueueSig := make(chan struct{}, 1)
	closeSig := make(chan struct{}, 1)

	gq := GroupQueue[T]{
		size:            size,
		maxPage:         maxPage,
		name:            name,
		fileDir:         fileDir,
		queues:          queues,
		nameIndices:     sync.Map{},
		groupSize:       groupSize,
		queueType:       queueType,
		queueCount:      0,
		closeCount:      0,
		activeQueue:     0,
		encoder:         encoder,
		decoder:         decoder,
		initializeBlock: make(chan struct{}),
		enqueueSig:      enqueueSig,
		closeSig:        closeSig,
		failureSig:      make(chan struct{}),
	}

	gq.isClose.Store(false)

	go func() {
		gq.initialize()
	}()

	return &gq, nil
}

func (gq *GroupQueue[T]) addQueue(name string) error {
	q, err := gq.registerQueue(name)
	if err != nil {
		return err
	}
	return gq.awaitChild(q)
}

func (gq *GroupQueue[T]) registerQueue(name string) (*Queue[T], error) {
	gq.addMu.Lock()
	defer gq.addMu.Unlock()
	if gq.isClose.Load() {
		return nil, ErrQueueClose
	}
	if failure := gq.failure.Load(); failure != nil {
		return nil, failure.err
	}
	if q, err := gq.getQueue(name); err == nil {
		return q, nil
	}
	if int(atomic.LoadUint32(&gq.queueCount)) >= gq.groupSize {
		return nil, fmt.Errorf("reached group queue max size, %d", gq.groupSize)
	}
	q, err := newQueue[T](name, gq, WithFileDir(filepath.Join(gq.fileDir, name)), WithMaxPage(gq.maxPage), WithQueueSize(gq.size), WithEncoder(gq.encoder), WithDecoder(gq.decoder), WithQueueType(gq.queueType))
	if err != nil {
		return nil, err
	}
	gq.mu.Lock()
	if failure := gq.failure.Load(); failure != nil {
		gq.mu.Unlock()
		q.CloseQueue()
		q.CloseIndex()
		return nil, failure.err
	}
	if gq.isClose.Load() {
		gq.mu.Unlock()
		q.CloseQueue()
		q.CloseIndex()
		return nil, ErrQueueClose
	}
	index := atomic.LoadUint32(&gq.queueCount)
	gq.queues[index] = q
	gq.nameIndices.Store(name, index)
	atomic.AddUint32(&gq.queueCount, 1)
	gq.mu.Unlock()
	if q.Length() > 0 || q.failure.Load() != nil {
		gq.signalEnqueue()
	}
	return q, nil
}

func (gq *GroupQueue[T]) awaitChild(q *Queue[T]) error {
	select {
	case <-q.initializeBlock:
		if failure := gq.failure.Load(); failure != nil {
			return failure.err
		}
		return q.initErr
	case <-gq.failureSig:
		return gq.failure.Load().err
	}
}

// Enqueue adds a single item to the queue identified by the given name.
// If the queue does not exist, it will be created automatically.
//
// Parameters:
//   - name: The name of the individual queue to which the item should be enqueued.
//   - item: A pointer to the data item to be enqueued.
//
// Returns:
//   - error: An error if the enqueue operation fails.
//
// Example:
//
//	// Enqueue a Data item into the queue named "queue1".
//	data := Data{...}
//	if err := gq.Enqueue("queue1", &data); err != nil {
//	    log.Fatalf("Enqueue failed: %v", err)
//	}
func (gq *GroupQueue[T]) Enqueue(name string, item *T) error {
	if failure := gq.failure.Load(); failure != nil {
		return failure.err
	}
	if gq.isClose.Load() {
		return ErrQueueClose
	}
	var err error
	q, err := gq.getQueue(name)
	if err != nil {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q, _ = gq.getQueue(name)
	}
	err = q.Enqueue(item)
	if err != nil {
		return err
	}
	gq.signalEnqueue()
	return nil
}

// BulkEnqueue adds multiple items to the queue identified by the given name.
// The items are enqueued in batches, respecting the individual queue's size limit.
//
// Parameters:
//   - name: The name of the individual queue to which the items should be enqueued.
//   - items: A slice of pointers to data items to be enqueued.
//
// Returns:
//   - error: An error if the bulk enqueue operation fails.
//
// Example:
//
//	// Enqueue multiple Data items into the queue named "queue1".
//	dataItems := []*Data{
//	    { ... },
//	    { ... },
//	}
//	if err := gq.BulkEnqueue("queue1", dataItems); err != nil {
//	    log.Fatalf("BulkEnqueue failed: %v", err)
//	}
func (gq *GroupQueue[T]) BulkEnqueue(name string, items []*T) error {
	if failure := gq.failure.Load(); failure != nil {
		return failure.err
	}
	if gq.isClose.Load() {
		return ErrQueueClose
	}
	var err error
	q, err := gq.getQueue(name)
	if err != nil {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q, _ = gq.getQueue(name)
	}
	var i uint64
	itemLength := uint64(len(items))
	for i < itemLength {
		if failure := gq.failure.Load(); failure != nil {
			return failure.err
		}
		var next uint64 = i + q.size - q.Length()
		if next == i {
			time.Sleep(30 * time.Microsecond)
			continue
		}
		if next >= itemLength {
			err = q.BulkEnqueue(items[i:])
		} else {
			err = q.BulkEnqueue(items[i:next])
		}
		if err != nil {
			return err
		}
		gq.signalEnqueue()
		i = next
	}
	return nil
}

// Dequeue retrieves a single item from the active queue in the group.
// It continuously monitors the underlying queues until an item is available,
// or the group queue has been closed.
//
// Returns:
//   - *Message[T]: The dequeued message containing the data item and metadata.
//   - error: An error if the dequeue operation fails or if the group queue is closed.
//
// Example:
//
//	// Dequeue a message from the group queue.
//	msg, err := gq.Dequeue()
//	if err != nil {
//	    log.Fatalf("Dequeue failed: %v", err)
//	}
//	fmt.Printf("Dequeued message: %+v\n", msg)
func (gq *GroupQueue[T]) Dequeue() (*Message[T], error) {
	// if queue has been closed, return ErrQueueClose
	for {
		err := gq.checkQueueSignal()
		if err != nil {
			return nil, err
		}
		q, _ := gq.getActiveQueue()
		if q == nil {
			continue
		}
		m, err := q.Dequeue()
		gq.signalEnqueue()
		if m != nil || err != nil && !IsErrQueueClose(err) {
			return m, err
		}
		gq.manageQueueClose()
	}
}

// BulkDequeue retrieves multiple items from the active queue in the group,
// returning a batch of messages once the specified size is reached or after the lazy duration expires.
//
// Parameters:
//   - size: The maximum number of items to dequeue in one batch. Zero returns ErrQueueOption without consuming messages.
//   - lazy: The duration to wait between dequeue attempts before returning the batch.
//
// Returns:
//   - []*Message[T]: A slice of dequeued messages.
//   - error: An error if the bulk dequeue operation fails or if the group queue is closed.
//
// Example:
//
//	// Dequeue a batch of 10 messages, waiting up to 100 milliseconds between attempts.
//	messages, err := gq.BulkDequeue(10, 100*time.Millisecond)
//	if err != nil {
//	    log.Fatalf("BulkDequeue failed: %v", err)
//	}
//	for _, msg := range messages {
//	    fmt.Printf("Dequeued message: %+v\n", msg)
//	}
func (gq *GroupQueue[T]) BulkDequeue(size uint64, lazy time.Duration) ([]*Message[T], error) {
	if size == 0 {
		return nil, fmt.Errorf("%w: bulk size must be greater than 0", ErrQueueOption)
	}
	var err error
	var count uint32
	for count == 0 {
		err = gq.checkQueueSignal()
		if err != nil {
			return nil, err
		}
		count = atomic.LoadUint32(&gq.queueCount)
	}

	// add enqueueSignal because get enqueueSignal first
	ms := make([]*Message[T], 0, size)
	batch := size / uint64(count)
	if batch == 0 {
		batch = 1
	}
	gq.signalEnqueue() // use next time select case
	timer := time.After(lazy)
	for {
		select {
		case <-gq.failureSig:
			return ms, gq.failure.Load().err
		case <-timer:
			return ms, nil
		case <-gq.closeSig:
			gq.signalClose() // close next time
			return ms, nil
		case <-gq.enqueueSig:
			select {
			case <-gq.closeSig:
				gq.signalClose() // close next time
				return ms, nil
			default:
			}

			q, _ := gq.getActiveQueue()
			if q == nil {
				continue
			}
			n := batch
			qlen := int(q.Length())
			if qlen == 0 {
				_, dequeueErr := q.Dequeue()
				if dequeueErr != nil && !IsErrQueueClose(dequeueErr) {
					return ms, dequeueErr
				}
				gq.manageQueueClose()
				gq.signalEnqueue()
				continue
			}
			if newN := uint64(qlen); newN < n {
				n = newN
			}
			if newN := size - uint64(len(ms)); newN < n {
				n = newN
			}
			for i := uint64(0); i < n; i++ {
				m, dequeueErr := q.Dequeue()
				if dequeueErr != nil && !IsErrQueueClose(dequeueErr) {
					return nil, dequeueErr
				}
				if m != nil {
					ms = append(ms, m)
				} else {
					gq.manageQueueClose()
				}
			}
			gq.signalEnqueue()
			if uint64(len(ms)) == size {
				return ms, nil
			}
		}
	}
}

// FuncAfterDequeue applies the given function to a single dequeued item.
// After processing the item, the queue index is updated accordingly.
//
// Parameters:
//   - f: A function that processes the dequeued data item.
//     It should return an error if processing fails.
//
// Returns:
//   - error: An error if either the dequeue operation, the function application, or the index update fails.
//
// Example:
//
//	err := gq.FuncAfterDequeue(func(data *Data) error {
//	    fmt.Println("Processed:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatalf("FuncAfterDequeue failed: %v", err)
//	}
func (gq *GroupQueue[T]) FuncAfterDequeue(f func(*T) error) error {
	// if queue has been closed, return ErrQueueClose
	for {
		err := gq.checkQueueSignal()
		if err != nil {
			return err
		}
		q, _ := gq.getActiveQueue()
		if q == nil {
			continue
		}
		m, dequeueErr := q.Dequeue()
		if dequeueErr != nil && !IsErrQueueClose(dequeueErr) {
			return dequeueErr
		}
		gq.signalEnqueue()
		if m != nil {
			var err error
			fErr := f(m.item)
			if fErr != nil {
				err = errors.Join(err, fErr)
			}
			iErr := q.UpdateIndex(m)
			if iErr != nil {
				err = errors.Join(err, iErr)
			}
			return err
		}
		gq.manageQueueClose()
	}
}

// FuncAfterBulkDequeue applies the given function to a batch of dequeued items.
// Once the batch is processed, it updates the indices of the corresponding queues.
//
// Parameters:
//   - size: The maximum number of items to dequeue in one batch. Zero returns ErrQueueOption without invoking f.
//   - lazy: The duration to wait between dequeue attempts before processing the batch.
//   - f: A function that processes the batch of data items.
//     It should return an error if processing fails.
//
// Returns:
//   - int: The actual number of items processed.
//   - error: An error if the bulk dequeue operation, batch processing, or index updates fail.
//
// Example:
//
//	count, err := gq.FuncAfterBulkDequeue(10, 100*time.Millisecond, func(data []*Data) error {
//	    fmt.Println("Processed batch:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatalf("FuncAfterBulkDequeue failed: %v", err)
//	}
//	fmt.Printf("Processed %d items\n", count)
func (gq *GroupQueue[T]) FuncAfterBulkDequeue(size uint64, lazy time.Duration, f func([]*T) error) (int, error) {
	if size == 0 {
		return 0, fmt.Errorf("%w: bulk size must be greater than 0", ErrQueueOption)
	}
	var err error
	var count uint32
	for count == 0 {
		err = gq.checkQueueSignal()
		if err != nil {
			return 0, err
		}
		count = atomic.LoadUint32(&gq.queueCount)
	}

	// add enqueueSignal because get enqueueSignal first
	items := make([]*T, 0, size)
	batch := size / uint64(count)
	if batch == 0 {
		batch = 1
	}
	gq.signalEnqueue() // next time select case
	lastMessageMap := make(map[uint32]*Message[T], gq.maxPage)
	timer := time.After(lazy)
LOOP:
	for {
		select {
		case <-gq.failureSig:
			return 0, gq.failure.Load().err
		case <-timer:
			break LOOP
		case <-gq.closeSig:
			gq.signalClose() // close next time
			break LOOP
		case <-gq.enqueueSig:
			select {
			case <-gq.closeSig:
				gq.signalClose() // close next time
				break LOOP
			default:
			}

			q, activeQueue := gq.getActiveQueue()
			if q == nil {
				continue
			}
			n := batch
			qlen := int(q.Length())
			if qlen == 0 {
				_, dequeueErr := q.Dequeue()
				if dequeueErr != nil && !IsErrQueueClose(dequeueErr) {
					return 0, dequeueErr
				}
				gq.manageQueueClose()
				gq.signalEnqueue()
				continue
			}
			if newN := uint64(qlen); newN < n {
				n = newN
			}
			if newN := size - uint64(len(items)); newN < n {
				n = newN
			}
			for i := uint64(0); i < n; i++ {
				m, dequeueErr := q.Dequeue()
				if dequeueErr != nil && !IsErrQueueClose(dequeueErr) {
					return 0, dequeueErr
				}
				if m != nil {
					items = append(items, m.item)
					lastMessageMap[activeQueue] = m
				} else {
					gq.manageQueueClose()
				}
			}
			gq.signalEnqueue()
			if uint64(len(items)) == size {
				break LOOP
			}
		}
	}
	fErr := f(items)
	if fErr != nil {
		err = errors.Join(err, fErr)
	}
	for i, message := range lastMessageMap {
		q := gq.queues[i]
		iErr := q.UpdateIndex(message)
		if iErr != nil {
			err = errors.Join(err, iErr)
		}
	}
	return len(items), err
}

// Length returns the lengths of each individual queue in the group as well as the total number
// of unprocessed items across all queues.
//
// Returns:
//   - []uint64: A slice containing the length of each queue.
//   - uint64: The total number of unprocessed items in the group.
//
// Example:
//
//	lengths, total := gq.Length()
//	fmt.Printf("Queue lengths: %v, Total items: %d\n", lengths, total)
func (gq *GroupQueue[T]) Length() ([]uint64, uint64) {
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	qls := make([]uint64, 0, gq.groupSize)
	var total uint64 = 0
	for _, q := range gq.queues {
		if q == nil {
			break
		}
		ql := q.Length()
		qls = append(qls, ql)
		total += ql
	}
	return qls, total
}

func (gq *GroupQueue[T]) initialize() {
	defer close(gq.initializeBlock)
	entries, err := os.ReadDir(gq.fileDir)
	if err != nil {
		gq.fail(fmt.Errorf("initialize group queue: cannot list directory"))
		return
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			count++
		}
	}
	if count > gq.groupSize {
		gq.fail(fmt.Errorf("initialize group queue: reached max size"))
		return
	}
	var wg sync.WaitGroup
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			if err := gq.addQueue(name); err != nil {
				gq.fail(err)
			}
		}(entry.Name())
	}
	wg.Wait()
}

// WaitInitialize blocks until the group queue has completed its initialization process.
// Start the consumer before waiting and start producers afterward.
// This method does not report initialization failures. Use WaitInitializeWithError
// when startup must check the initialization result.
//
// Example:
//
//	// Start the consumer before waiting for initialization to complete.
//	gq.WaitInitialize()
//	// Completion alone does not indicate success; check operation errors.
func (gq *GroupQueue[T]) WaitInitialize() {
	<-gq.initializeBlock
}

// WaitInitializeWithError waits for group initialization to finish, then returns
// a recorded failure or ErrQueueClose if the group is closed. Start the consumer
// before waiting. On success, the children discovered at startup are initialized;
// children created by later enqueues still initialize separately.
// A nil result does not guarantee that later operations will succeed or that
// recovered messages have finished processing. Continue checking operation errors.
// A failure does not guarantee that all child I/O has stopped.
//
// Example:
//
//	// Start the consumer first. Start producers only after this succeeds.
//	if err := gq.WaitInitializeWithError(); err != nil {
//	    panic(err)
//	}
func (gq *GroupQueue[T]) WaitInitializeWithError() error {
	<-gq.initializeBlock
	if failure := gq.failure.Load(); failure != nil {
		return failure.err
	}
	if gq.isClose.Load() {
		return ErrQueueClose
	}
	return nil
}

func (gq *GroupQueue[T]) signalEnqueue() {
	select {
	case gq.enqueueSig <- struct{}{}:
	default:
	}
}

func (gq *GroupQueue[T]) signalClose() {
	select {
	case gq.closeSig <- struct{}{}:
	default:
	}
}

func (gq *GroupQueue[T]) getQueue(name string) (*Queue[T], error) {
	// Each slot is immutable after registration. Store publishes the slot
	// before its index is made visible through nameIndices.Load.
	i, ok := gq.nameIndices.Load(name)
	if !ok {
		return nil, fmt.Errorf("queue name: %s, %w", name, ErrQueueNotFound)
	}
	return gq.queues[i.(uint32)], nil
}

// CloseQueue closes all individual queues within the group and signals that no further enqueues are allowed.
//
// Returns:
//   - error: An error if any of the queues fail to close.
//
// Example:
//
//	if err := gq.CloseQueue(); err != nil {
//	    log.Fatalf("CloseQueue failed: %v", err)
//	}
func (gq *GroupQueue[T]) CloseQueue() error {
	gq.mu.Lock()
	gq.isClose.Store(true)
	queues := append([]*Queue[T](nil), gq.queues[:atomic.LoadUint32(&gq.queueCount)]...)
	gq.mu.Unlock()
	var err error
	for _, q := range queues {
		err = errors.Join(err, q.CloseQueue())
	}
	if len(queues) == 0 {
		gq.signalClose()
	}
	gq.signalEnqueue()
	return err
}

func (gq *GroupQueue[T]) manageQueueClose() {
	closeCount := atomic.AddUint32(&gq.closeCount, 1)
	queueCount := atomic.LoadUint32(&gq.queueCount)
	if closeCount == queueCount {
		gq.signalClose()
	}
}

// CloseIndex closes the index files associated with all individual queues in the group.
//
// Returns:
//   - error: An error if any of the index files fail to close.
//
// Example:
//
//	if err := gq.CloseIndex(); err != nil {
//	    log.Fatalf("CloseIndex failed: %v", err)
//	}
func (gq *GroupQueue[T]) CloseIndex() error {
	gq.mu.RLock()
	queues := append([]*Queue[T](nil), gq.queues[:atomic.LoadUint32(&gq.queueCount)]...)
	gq.mu.RUnlock()
	var err error
	for _, q := range queues {
		err = errors.Join(err, q.CloseIndex())
	}
	return err
}

// UpdateIndex updates the index of the given message in its corresponding individual queue.
// This is used to record the position up to which the queue has been processed.
//
// Parameters:
//   - m: The message whose index should be updated. The message's name is used to identify the corresponding queue.
//
// Returns:
//   - error: An error if the index update fails.
//
// Example:
//
//	if err := gq.UpdateIndex(message); err != nil {
//	    log.Fatalf("UpdateIndex failed: %v", err)
//	}
func (gq *GroupQueue[T]) UpdateIndex(m *Message[T]) error {
	if m == nil {
		return fmt.Errorf("queue progress: nil message")
	}
	q, err := gq.getQueue(m.Name())
	if err != nil {
		return err
	}
	return q.UpdateIndex(m)
}

func (gq *GroupQueue[T]) getActiveQueue() (*Queue[T], uint32) {
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	count := atomic.LoadUint32(&gq.queueCount)
	if count == 0 {
		return nil, 0
	}
	start := atomic.LoadUint32(&gq.activeQueue) % count
	for offset := uint32(0); offset < count; offset++ {
		i := (start + offset) % count
		q := gq.queues[i]
		if q.Length() > 0 || q.failure.Load() != nil || q.isQueueClosed.Load() && !q.isQueueClosedRecieved.Load() {
			atomic.StoreUint32(&gq.activeQueue, (i+1)%count)
			return q, i
		}
	}
	return nil, 0
}

func (gq *GroupQueue[T]) checkQueueSignal() error {
	if failure := gq.failure.Load(); failure != nil {
		return failure.err
	}
	select {
	case <-gq.failureSig:
		return gq.failure.Load().err
	case <-gq.closeSig:
		return ErrQueueClose
	case <-gq.enqueueSig:
		if failure := gq.failure.Load(); failure != nil {
			return failure.err
		}
		select {
		case <-gq.closeSig:
			return ErrQueueClose
		default:
			return nil
		}
	}
}

func (gq *GroupQueue[T]) fail(err error) error {
	if gq.failure.CompareAndSwap(nil, &queueFailure{err: err}) {
		close(gq.failureSig)
	}
	gq.signalEnqueue()
	return gq.failure.Load().err
}

func (gq *GroupQueue[T]) waitInitialize() error {
	<-gq.initializeBlock
	if failure := gq.failure.Load(); failure != nil {
		return failure.err
	}
	return nil
}
