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
	size            uint64 // The maximum number of items in the queue.
	maxPage         uint64
	name            string      // The name of the group queue.
	fileDir         string      // The directory where queue files are stored.
	queues          []*Queue[T] // A map of queue names to their corresponding Queue instances.
	nameIndices     sync.Map
	groupSize       int
	queueType       QueueType
	queueCount      uint32
	closeCount      uint32
	activeQueue     uint32
	isClose         atomic.Bool
	encoder         func(v any) ([]byte, error)    // Function to encode data before saving to the queue.
	decoder         func(data []byte, v any) error // Function to decode data when reading from the queue.
	initializeBlock chan struct{}                  // A channel to block until the queue is fully initialized.
	enqueueSig      chan struct{}                  // A signal channel to notify enqueue operations.
	closeSig        chan struct{}                  // A signal channel to notify that the queue is closed.
	mu              sync.RWMutex
}

// NewGroupQueue initializes a new GroupQueue with the given name and options.
// It sets up the queues, directory, encoder, and decoder for managing the queue data.
//
// Parameters:
//   - name: The name of the group queue.
//   - opts: Optional settings for the group queue.
//
// Returns:
//   - *GroupQueue: A pointer to the newly created GroupQueue.
//   - error: An error if any occurs during the queue creation.
//
// Example:
//
//	gq, err := NewGroupQueue[Data]("myGroupQueue", WithQueueSize(100), WithMaxPages(5))
//	if err != nil {
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
//   - name: The name of the queue to which the data should be enqueued.
//   - data: The data item to be added to the queue.
//
// Returns:
//   - error: An error if the enqueue operation fails.
//
// Example:
//
//	 data := Data{...}
//		err := gq.Enqueue("queue1", &data)
//		if err != nil {
//		    log.Fatal(err)
//		}
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
// The items are added in batches, ensuring the queue size limit is respected.
//
// Parameters:
//   - name: The name of the queue to which the data should be enqueued.
//   - data: A slice of data items to be added to the queue.
//
// Returns:
//   - error: An error if the bulk enqueue operation fails.
//
// Example:
//
//	data := []*Data{{...},{...},...}
//	err := gq.BulkEnqueue("queue1", data)
//
//	if err != nil {
//	    log.Fatal(err)
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

// Dequeue retrieves items from all non-empty queues in the GroupQueue.
// The items are sent to a channel for further processing.
//
// Returns:
//   - chan *Message[T]: A channel from which dequeued items can be received.
//   - error: An error if the dequeue operation fails.
//
// Example:
//
//	mCh, err := gq.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for m := range mCh {
//	    fmt.Println(m)
//	}
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

// BulkDequeue retrieves multiple items from all non-empty queues in the GroupQueue
// and sends them in batches of the specified size.
//
// Parameters:
//   - size: The number of items to dequeue in each batch.
//   - lazy: A duration to wait between dequeue operations.
//
// Returns:
//   - chan []*Message[T]: A channel from which batches of dequeued items can be received.
//   - error: An error if the bulk dequeue operation fails.
//
// Example:
//
//	msCh, err := gq.BulkDequeue(10, 100*time.Millisecond)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for ms := range msCh {
//	    fmt.Println(ms)
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

// FuncAfterDequeue applies a given function to each item after it is dequeued.
//
// Parameters:
//   - f: A function that will be applied to each dequeued item.
//
// Returns:
//   - error: An error if the operation fails.
//
// Example:
//
//	err := gq.FuncAfterDequeue(func(data *T) error {
//	    fmt.Println("Processed:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatal(err)
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

// FuncAfterBulkDequeue applies a given function to multiple items after they are dequeued in batches.
//
// Parameters:
//   - size: The number of items to dequeue in each batch.
//   - lazy: A duration to wait between dequeue operations.
//   - f: A function that will be applied to each batch of dequeued items.
//
// Returns:
//   - error: An error if the operation fails.
//
// Example:
//
//	err := gq.FuncAfterBulkDequeue(10, 100*time.Millisecond, func(data []*T) error {
//	    fmt.Println("Processed batch:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
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

// WaitInitialize blocks until the group queue is fully initialized.
//
// Example:
//
//	gq, _ := NewGroupQueue(...)
//	// start dequeue
//	go func(){
//		for {
//			mCh, _ := gq.Dequeue()
//		}
//	}
//	gq.WaitInitialize()
//	go func() {
//		gq.Enqueu(data)
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

// CloseQueue closes all queues in the group and signals the closure.
//
// Returns:
//   - error: An error if any of the queues fail to close.
//
// Example:
//
//	err := gq.CloseQueue()
//	if err != nil {
//	    log.Fatal(err)
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

// CloseIndex closes the index files associated with all queues in the group.
//
// Returns:
//   - error: An error if any of the index files fail to close.
//
// Example:
//
//	err := gq.CloseIndex()
//	if err != nil {
//	    log.Fatal(err)
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

// UpdateIndex updates the index of a given m in its corresponding queue.
//
// Parameters:
//   - m: The m whose index needs to be updated.
//
// Returns:
//   - error: An error if the update fails.
//
// Example:
//
//	err := gq.UpdateIndex(m)
//	if err != nil {
//	    log.Fatal(err)
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
