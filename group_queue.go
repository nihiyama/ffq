// Package ffq provides a File-based FIFO Queue implementation that supports generic types.
package ffq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
	mu              sync.RWMutex                   // Mutex to synchronize access to the group's queues.
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
//	// Create a group queue for Data items with a queue size of 1024 and up to 5 pages.
//	gq, err := ffq.NewGroupQueue[Data]("myGroupQueue",
//	     ffq.WithQueueSize(1024),
//	     ffq.WithMaxPage(2),
//	     ffq.WithGroupSize(10),
//	     ffq.WithQueueType(ffq.SPSC))
//	if err != nil {
//	    log.Fatalf("Failed to create group queue: %v", err)
//	}
//	// Wait until the group queue is fully initialized before use.
//	gq.WaitInitialize()
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
	}

	gq.isClose.Store(false)

	go func() {
		gq.initialize()
	}()

	return &gq, nil
}

func (gq *GroupQueue[T]) addQueue(name string) error {
	if gq.isClose.Load() {
		return fmt.Errorf("already closed")
	}
	q, err := NewQueue[T](
		name,
		WithFileDir(filepath.Join(gq.fileDir, name)),
		WithMaxPage(gq.maxPage),
		WithQueueSize(gq.size),
		WithEncoder(gq.encoder),
		WithDecoder(gq.decoder),
		WithQueueType(gq.queueType),
	)
	if err != nil {
		return err
	}
	queueCount := atomic.AddUint32(&gq.queueCount, 1)
	if int(queueCount) > gq.groupSize {
		q.mu.Lock()
		q.WaitInitialize()
		os.RemoveAll(q.fileDir)
		q.mu.Unlock()
		return fmt.Errorf("reached group queue max size, %d", gq.groupSize)
	}
	gq.nameIndices.Store(name, queueCount-1)
	gq.mu.Lock()
	gq.queues[queueCount-1] = q
	gq.mu.Unlock()
	q.WaitInitialize()
	if q.Length() > 0 {
		gq.signalEnqueue()
	}
	return nil
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
			return nil, ErrQueueClose
		}
		q, _ := gq.getActiveQueue()
		m, err := q.Dequeue()
		gq.signalEnqueue()
		if m != nil {
			return m, err
		}
		gq.manageQueueClose()
	}
}

// BulkDequeue retrieves multiple items from the active queue in the group,
// returning a batch of messages once the specified size is reached or after the lazy duration expires.
//
// Parameters:
//   - size: The number of items to dequeue in one batch.
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
	err := gq.checkQueueSignal()
	if err != nil {
		return nil, ErrQueueClose
	}

	// add enqueueSignal because get enqueueSignal first
	ms := make([]*Message[T], 0, size)
	batch := size / uint64(atomic.LoadUint32(&gq.queueCount))
	gq.signalEnqueue() // use next time select case
	timer := time.After(lazy)
	for {
		select {
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
			n := batch
			qlen := int(q.Length() + 1) // if q.Length() == 0, queue may be closed.
			if newN := uint64(qlen); newN < n {
				n = newN
			}
			if newN := size - uint64(len(ms)); newN < n {
				n = newN
			}
			for i := uint64(0); i < n; i++ {
				m, _ := q.Dequeue()
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
			return ErrQueueClose
		}
		q, _ := gq.getActiveQueue()
		m, _ := q.Dequeue()
		gq.signalEnqueue()
		if m != nil {
			var err error
			fErr := f(m.item)
			if fErr != nil {
				err = errors.Join(err, fErr)
			}
			iErr := q.writeIndex(m.index)
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
//   - size: The maximum number of items to dequeue in one batch.
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
	err := gq.checkQueueSignal()
	if err != nil {
		return 0, ErrQueueClose
	}

	// add enqueueSignal because get enqueueSignal first
	items := make([]*T, 0, size)
	batch := size / uint64(atomic.LoadUint32(&gq.queueCount))
	gq.signalEnqueue() // next time select case
	lastIndexMap := make(map[uint32]uint64, gq.maxPage)
	timer := time.After(lazy)
LOOP:
	for {
		select {
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
			n := batch
			qlen := int(q.Length() + 1) // if q.Length() == 0, queue may be closed.
			if newN := uint64(qlen); newN < n {
				n = newN
			}
			if newN := size - uint64(len(items)); newN < n {
				n = newN
			}
			for i := uint64(0); i < n; i++ {
				m, _ := q.Dequeue()
				if m != nil {
					items = append(items, m.item)
					lastIndexMap[activeQueue] = m.index
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
	for i, index := range lastIndexMap {
		q := gq.queues[i]
		iErr := q.writeIndex(index)
		if iErr != nil {
			err = errors.Join(err, iErr)
		}
	}
	return len(items), nil
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
	entries, err := os.ReadDir(gq.fileDir)
	if err != nil {
		panic(fmt.Sprintf("could not find directory, %s, %v", gq.fileDir, err))
	}

	var wg sync.WaitGroup

	for _, entry := range entries {
		if entry.IsDir() {
			wg.Add(1)
			go func(wg *sync.WaitGroup, entry fs.DirEntry) {
				defer wg.Done()
				gq.addQueue(entry.Name())
			}(&wg, entry)
		}
	}
	// wait goroutine
	wg.Wait()

	// release blocking
	gq.initializeBlock <- struct{}{}
}

// WaitInitialize blocks until the group queue has completed its initialization process.
// This ensures that all individual queues have been set up (for example, by reading existing data files)
// before the group queue is used.
//
// Example:
//
//	// Create a new group queue and wait for initialization to complete.
//	gq, err := ffq.NewGroupQueue[Data]("myGroupQueue", ffq.WithQueueSize(1024))
//	if err != nil {
//	    log.Fatalf("Failed to create group queue: %v", err)
//	}
//	gq.WaitInitialize()
//	// Now it is safe to start enqueuing and dequeuing items.
//	go func() {
//	    for {
//	        msg, err := gq.Dequeue()
//	        if err != nil {
//	            log.Println("Dequeue error:", err)
//	            break
//	        }
//	        fmt.Println("Dequeued:", msg)
//	    }
//	}()
//	// Enqueue data into the group queue.
//	if err := gq.Enqueue("queue1", &dataItem); err != nil {
//	    log.Fatal(err)
//	}
func (gq *GroupQueue[T]) WaitInitialize() {
	<-gq.initializeBlock
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
	i, ok := gq.nameIndices.Load(name)
	if !ok {
		return nil, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound)
	}
	q := gq.queues[i.(uint32)]
	return q, nil
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
	var err error
	gq.isClose.Store(true)
	for _, q := range gq.queues {
		if q == nil {
			break
		}
		closeErr := q.CloseQueue()
		if err != nil {
			err = errors.Join(err, closeErr)
		}
	}
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
	var err error
	for _, q := range gq.queues {
		if q == nil {
			break
		}
		if q.isQueueClosed.Load() && !q.isIndexClosed.Load() {
			err = q.CloseIndex()
			atomic.AddUint32(&gq.closeCount, 1)
		}
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
	var err error
	q, gqErr := gq.getQueue(m.name)
	if gqErr != nil {
		err = errors.Join(err, gqErr)
	}
	iErr := q.writeIndex(m.index)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return err
}

func (gq *GroupQueue[T]) getActiveQueue() (*Queue[T], uint32) {
	activeQueue := atomic.LoadUint32(&gq.activeQueue)
	startActiveQueue := activeQueue
	for {
		gq.mu.RLock()
		q := gq.queues[activeQueue]
		gq.mu.RUnlock()
		if q == nil {
			activeQueue = 0
			continue
		}
		if !q.isQueueClosedRecieved.Load() && (q.Length() > 0 || q.isQueueClosed.Load()) {
			if activeQueue == uint32(gq.groupSize-1) {
				atomic.StoreUint32(&gq.activeQueue, 0)
			} else {
				atomic.StoreUint32(&gq.activeQueue, activeQueue+1)
			}
			return q, activeQueue
		}
		if activeQueue == uint32(gq.groupSize-1) {
			activeQueue = 0
		} else {
			activeQueue++
		}
		if activeQueue == startActiveQueue {
			time.Sleep(30 * time.Microsecond)
		}
	}
}

func (gq *GroupQueue[T]) checkQueueSignal() error {
	select {
	case <-gq.closeSig:
		return ErrQueueClose
	case <-gq.enqueueSig:
		select {
		case <-gq.closeSig:
			return ErrQueueClose
		default:
			return nil
		}
	}
}
