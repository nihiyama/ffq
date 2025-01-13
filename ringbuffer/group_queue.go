// Package ffq provides a File-based FIFO Queue implementation that supports generic types.
package ringbuffer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// GroupQueue represents a group of queues with common configurations like size, encoder, and decoder.
// It manages multiple named queues and supports operations like enqueue, dequeue, and bulk enqueue/dequeue.
type GroupQueue[T any] struct {
	size            uint64 // The maximum number of items in the queue.
	maxPage         uint64
	name            string                         // The name of the group queue.
	fileDir         string                         // The directory where queue files are stored.
	queues          map[string]*Queue[T]           // A map of queue names to their corresponding Queue instances.
	encoder         func(v any) ([]byte, error)    // Function to encode data before saving to the queue.
	decoder         func(data []byte, v any) error // Function to decode data when reading from the queue.
	initializeBlock chan struct{}                  // A channel to block until the queue is fully initialized.
	enqueueSig      chan struct{}                  // A signal channel to notify enqueue operations.
	closeSig        chan struct{}                  // A signal channel to notify that the queue is closed.
	mu              *sync.RWMutex                  // A mutex to protect the map of queues.
}

type bulkQueueChData[T any] struct {
	items   []*T
	indices map[string]uint64
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

	var encoder func(v any) ([]byte, error) = json.Marshal
	if options.encoder != nil {
		encoder = *options.encoder
	}

	var decoder func(data []byte, v any) error = json.Unmarshal
	if options.decoder != nil {
		decoder = *options.decoder
	}

	queues := make(map[string]*Queue[T], 10)
	var mu sync.RWMutex
	enqueueSig := make(chan struct{}, 1)
	closeSig := make(chan struct{}, 1)

	gq := GroupQueue[T]{
		size:            size,
		maxPage:         maxPage,
		name:            name,
		fileDir:         fileDir,
		queues:          queues,
		encoder:         encoder,
		decoder:         decoder,
		initializeBlock: make(chan struct{}),
		mu:              &mu,
		enqueueSig:      enqueueSig,
		closeSig:        closeSig,
	}

	go func() {
		gq.initialize()
	}()

	return &gq, nil
}

func (gq *GroupQueue[T]) addQueue(name string) error {
	q, err := NewQueue[T](
		name,
		WithFileDir(filepath.Join(gq.fileDir, name)),
		WithMaxPage(gq.maxPage),
		WithQueueSize(gq.size),
		WithEncoder(gq.encoder),
		WithDecoder(gq.decoder),
	)
	if err != nil {
		return err
	}
	gq.mu.Lock()
	gq.queues[name] = q
	gq.mu.Unlock()
	q.WaitInitialize()
	if q.Length() > 0 {
		gq.sendSignal()
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
	gq.sendSignal()
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
	ld := uint64(len(items))
	for i < ld {
		var next uint64 = i + q.size - q.Length()
		if next == i {
			time.Sleep(30 * time.Microsecond)
			continue
		}
		if next >= ld {
			err = q.BulkEnqueue(items[i:])
		} else {
			err = q.BulkEnqueue(items[i:next])
		}
		if err != nil {
			return err
		}
		gq.sendSignal()
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
func (gq *GroupQueue[T]) Dequeue() (chan *Message[T], error) {
	var err error
	var errMu sync.Mutex
	mCh := make(chan *Message[T])
	// if queue has been closed, return ErrQueueClose
	select {
	case <-gq.enqueueSig:
	case <-gq.closeSig:
		select {
		case <-gq.enqueueSig:
			gq.closeSig <- struct{}{}
		default:
			return nil, ErrQueueClose
		}
	}
	nameQueueLenMap := gq.lengthWithNotEmpty()
	go func() {
		defer close(mCh)
		var queueWg sync.WaitGroup
		for name, length := range nameQueueLenMap {
			// no err will occur because queue existence's is guaranteed.
			q, _ := gq.getQueue(name)
			queueWg.Add(1)
			go func(wg *sync.WaitGroup, name string, length uint64, queue *Queue[T]) {
				defer wg.Done()
				var i uint64
				for i = 0; i < length; i++ {
					m, qErr := q.Dequeue()
					if qErr != nil {
						errMu.Lock()
						err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
						errMu.Unlock()
					}
					mCh <- m
				}
			}(&queueWg, name, length, q)
		}
		queueWg.Wait()
	}()
	return mCh, err
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
func (gq *GroupQueue[T]) BulkDequeue(size int, lazy time.Duration) (chan []*Message[T], error) {
	var err error
	var errMu sync.Mutex
	msCh := make(chan []*Message[T])
	var ms []*Message[T]
	var mMu sync.Mutex

	appendMessages := func(m *Message[T]) {
		mMu.Lock()
		defer mMu.Unlock()
		ms = append(ms, m)
		if len(ms) == size {
			sendMs := make([]*Message[T], len(ms))
			copy(sendMs, ms)
			msCh <- sendMs
			ms = make([]*Message[T], 0, size)
		}
	}
	resetMessages := func() {
		mMu.Lock()
		defer mMu.Unlock()
		ms = make([]*Message[T], 0, size)
	}

	// if queue has been closed, return ErrQueueClose
	select {
	case <-gq.enqueueSig:
	case <-gq.closeSig:
		select {
		case <-gq.enqueueSig:
			gq.closeSig <- struct{}{}
		default:
			return nil, ErrQueueClose
		}
	}
	go func() {
		defer close(msCh)
		timer := time.After(lazy)
		resetMessages()
		// add enqueueSignal because get enqueueSignal first
		gq.sendSignal()
		for {
			select {
			case <-timer:
				msCh <- ms
				return
			case <-gq.enqueueSig:
				nameQueueLenMap := gq.lengthWithNotEmpty()
				var queueWg sync.WaitGroup
				for name, length := range nameQueueLenMap {
					// no err will occur because queue existence's is guaranteed.
					q, _ := gq.getQueue(name)
					queueWg.Add(1)
					go func(wg *sync.WaitGroup, name string, length uint64, queue *Queue[T]) {
						defer wg.Done()
						var i uint64
						for i = 0; i < length; i++ {
							m, qErr := q.Dequeue()
							if qErr != nil {
								errMu.Lock()
								err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
								errMu.Unlock()
							}
							appendMessages(m)
						}
					}(&queueWg, name, length, q)
				}
				queueWg.Wait()
			}
		}
	}()
	return msCh, err
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
	var err error
	var errMu sync.Mutex

	// if queue has been closed, return ErrQueueClose
	select {
	case <-gq.enqueueSig:
	case <-gq.closeSig:
		select {
		case <-gq.enqueueSig:
			gq.closeSig <- struct{}{}
		default:
			return ErrQueueClose
		}
	}
	// check name and queue length
	nameQueueLenMap := gq.lengthWithNotEmpty()
	var queueWg sync.WaitGroup
	for name, length := range nameQueueLenMap {
		// no err will occur because queue existence's is guaranteed.
		q, _ := gq.getQueue(name)
		queueWg.Add(1)
		go func(wg *sync.WaitGroup, name string, length uint64, queue *Queue[T]) {
			var i uint64
			defer wg.Done()
			for i = 0; i < length; i++ {
				m, qErr := q.Dequeue()
				if qErr != nil {
					errMu.Lock()
					err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
					errMu.Unlock()
				}
				fErr := f(m.item)
				if fErr != nil {
					errMu.Lock()
					err = errors.Join(err, fErr)
					errMu.Unlock()
				}
				iErr := q.writeIndex(m.index)
				if iErr != nil {
					errMu.Lock()
					err = errors.Join(err, iErr)
					errMu.Unlock()
				}
			}
		}(&queueWg, name, length, q)
	}
	queueWg.Wait()
	return err
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
func (gq *GroupQueue[T]) FuncAfterBulkDequeue(size int, lazy time.Duration, f func([]*T) error) error {
	var err error
	var errMu sync.Mutex
	itemsCh := make(chan bulkQueueChData[T])
	var items []*T
	var indices map[string]uint64
	var itemsMu sync.RWMutex

	appendData := func(m *Message[T], indexLength int) {
		itemsMu.Lock()
		defer itemsMu.Unlock()
		indices[m.name] = m.index
		items = append(items, m.item)
		if len(items) == size {
			// copy for send consumer
			sendItems := make([]*T, len(items))
			copy(sendItems, items)
			sendIndices := make(map[string]uint64, len(indices))
			for k, v := range indices {
				sendIndices[k] = v
			}
			itemsCh <- bulkQueueChData[T]{
				items:   sendItems,
				indices: sendIndices,
			}
			items = make([]*T, 0, size)
			indices = make(map[string]uint64, indexLength)
		}
	}
	resetData := func(indexLength int) {
		itemsMu.Lock()
		defer itemsMu.Unlock()
		items = make([]*T, 0, size)
		indices = make(map[string]uint64, indexLength)
	}

	// if queue has been closed, return ErrQueueClose
	select {
	case <-gq.enqueueSig:
	case <-gq.closeSig:
		select {
		case <-gq.enqueueSig:
			gq.closeSig <- struct{}{}
		default:
			return ErrQueueClose
		}
	}

	go func() {
		defer close(itemsCh)
		timer := time.After(lazy)
		nameQueueLenMap := gq.Length()
		resetData(len(nameQueueLenMap))
		// add enqueueSignal because get enqueueSignal first
		gq.sendSignal()
		for {
			select {
			case <-timer:
				itemsCh <- bulkQueueChData[T]{
					items:   items,
					indices: indices,
				}
				return
			case <-gq.enqueueSig:
				nameQueueLenMap := gq.lengthWithNotEmpty()
				var queueWg sync.WaitGroup
				for name, length := range nameQueueLenMap {
					// no err will occur because queue existence's is guaranteed.
					q, _ := gq.getQueue(name)
					queueWg.Add(1)
					go func(wg *sync.WaitGroup, name string, length uint64, queue *Queue[T]) {
						var i uint64
						defer wg.Done()
						for i = 0; i < length; i++ {
							m, qErr := q.Dequeue()
							if qErr != nil {
								errMu.Lock()
								err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
								errMu.Unlock()
							}
							appendData(m, len(nameQueueLenMap))
						}
					}(&queueWg, name, length, q)
				}
				queueWg.Wait()
			}
		}
	}()

	for d := range itemsCh {
		if len(d.items) == 0 {
			continue
		}
		fErr := f(d.items)
		if fErr != nil {
			errMu.Lock()
			err = errors.Join(err, fErr)
			errMu.Unlock()
		}
		for name, bulkIndex := range d.indices {
			q, gqErr := gq.getQueue(name)
			if gqErr != nil {
				errMu.Lock()
				err = errors.Join(err, gqErr)
				errMu.Unlock()
			}
			wiErr := q.writeIndex(bulkIndex)
			if wiErr != nil {
				errMu.Lock()
				err = errors.Join(err, wiErr)
				errMu.Unlock()
			}
		}
	}
	return err
}

func (gq *GroupQueue[T]) lengthWithNotEmpty() map[string]uint64 {
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	nameQueueLenMap := make(map[string]uint64, len(gq.queues))
	for name, q := range gq.queues {
		if ql := q.Length(); ql > 0 {
			nameQueueLenMap[name] = ql
		}
	}
	return nameQueueLenMap
}

func (gq *GroupQueue[T]) Length() map[string]uint64 {
	nameQueueLenMap := make(map[string]uint64, len(gq.queues))
	for name, q := range gq.queues {
		nameQueueLenMap[name] = q.Length()
	}
	return nameQueueLenMap
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

func (gq *GroupQueue[T]) sendSignal() {
	select {
	case gq.enqueueSig <- struct{}{}:
	default:
	}
}

func (gq *GroupQueue[T]) getQueue(name string) (*Queue[T], error) {
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	q, ok := gq.queues[name]
	if !ok {
		return q, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound)
	}
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
func (gq *GroupQueue[T]) CloseQueue() {
	gq.mu.Lock()
	defer gq.mu.Unlock()
	for _, q := range gq.queues {
		q.CloseQueue()
	}
	gq.closeSig <- struct{}{}
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
	gq.mu.Lock()
	defer gq.mu.Unlock()
	for name, q := range gq.queues {
		closeErr := q.indexFile.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, closeErr))
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
