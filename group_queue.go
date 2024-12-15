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
	"time"
)

// GroupQueue represents a group of queues with common configurations like size, encoder, and decoder.
// It manages multiple named queues and supports operations like enqueue, dequeue, and bulk enqueue/dequeue.
type GroupQueue[T any] struct {
	queueSize       int                            // The maximum size of each queue.
	maxPages        int                            // The maximum number of pages for each queue.
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
	data    []*T
	indices map[string]bulkIndicies
}

type bulkIndicies struct {
	page        int
	globalIndex int
	localIndex  int
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

	queueSize := 100
	if options.queueSize != nil {
		queueSize = *options.queueSize
	}

	maxPages := 2
	if options.maxPages != nil {
		maxPages = *options.maxPages
	}

	var encoder func(v any) ([]byte, error) = json.Marshal
	if options.encoder != nil {
		encoder = *options.encoder
	}

	var decoder func(data []byte, v any) error = json.Unmarshal
	if options.decoder != nil {
		decoder = *options.decoder
	}

	initializeBlock := make(chan struct{})
	queues := make(map[string]*Queue[T], 10)
	var mu sync.RWMutex
	enqueueSig := make(chan struct{}, 1)
	closeSig := make(chan struct{}, 1)

	gq := GroupQueue[T]{
		name:            name,
		fileDir:         fileDir,
		queueSize:       queueSize,
		maxPages:        maxPages,
		encoder:         encoder,
		decoder:         decoder,
		initializeBlock: initializeBlock,
		queues:          queues,
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
		WithMaxPages(gq.maxPages),
		WithQueueSize(gq.queueSize),
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
func (gq *GroupQueue[T]) Enqueue(name string, data *T) error {
	var err error
	q, err := gq.getQueue(name)
	if err != nil {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q, _ = gq.getQueue(name)
	}
	err = q.Enqueue(data)
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
func (gq *GroupQueue[T]) BulkEnqueue(name string, data []*T) error {
	var err error
	q, err := gq.getQueue(name)
	if err != nil {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q, _ = gq.getQueue(name)
	}
	i := 0
	ld := len(data)
	for i < ld {
		next := i + q.queueSize - q.Length()
		if next == i {
			time.Sleep(30 * time.Microsecond)
			continue
		}
		if next >= ld {
			err = q.BulkEnqueue(data[i:])
		} else {
			err = q.BulkEnqueue(data[i:next])
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
			go func(wg *sync.WaitGroup, name string, length int, queue *Queue[T]) {
				defer wg.Done()
				for i := 0; i < length; i++ {
					message, qErr := q.Dequeue()
					if qErr != nil {
						err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
					}
					mCh <- message
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
	msCh := make(chan []*Message[T])
	var messages []*Message[T]
	var messageMu sync.Mutex

	appendMessages := func(m *Message[T]) {
		messageMu.Lock()
		defer messageMu.Unlock()
		messages = append(messages, m)
		if len(messages) == size {
			msCh <- messages
			messages = make([]*Message[T], 0, size)
		}
	}
	resetMessages := func() {
		messageMu.Lock()
		defer messageMu.Unlock()
		messages = make([]*Message[T], 0, size)
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
				msCh <- messages
				return
			case <-gq.enqueueSig:
				nameQueueLenMap := gq.lengthWithNotEmpty()
				var queueWg sync.WaitGroup
				for name, length := range nameQueueLenMap {
					// no err will occur because queue existence's is guaranteed.
					q, _ := gq.getQueue(name)
					queueWg.Add(1)
					go func(wg *sync.WaitGroup, name string, length int, queue *Queue[T]) {
						defer wg.Done()
						for i := 0; i < length; i++ {
							message, qErr := q.Dequeue()
							if qErr != nil {
								err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
							}
							appendMessages(message)
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
		go func(wg *sync.WaitGroup, name string, length int, queue *Queue[T]) {
			defer wg.Done()
			for i := 0; i < length; i++ {
				message, qErr := q.Dequeue()
				if qErr != nil {
					err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
				}
				fErr := f(message.data)
				if fErr != nil {
					err = errors.Join(err, fErr)
				}
				iErr := q.writeIndex(message.page, message.globalIndex, message.localIndex)
				if iErr != nil {
					err = errors.Join(err, iErr)
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
	dataCh := make(chan bulkQueueChData[T])
	var data []*T
	var indices map[string]bulkIndicies
	var dataMu sync.Mutex

	appendData := func(m *Message[T]) {
		dataMu.Lock()
		defer dataMu.Unlock()
		indices[m.name] = bulkIndicies{
			page:        m.page,
			globalIndex: m.globalIndex,
			localIndex:  m.localIndex,
		}
		data = append(data, m.data)
		if len(data) == size {
			dataCh <- bulkQueueChData[T]{
				data:    data,
				indices: indices,
			}
			data = make([]*T, 0, size)
		}
	}
	resetData := func(indexLength int) {
		dataMu.Lock()
		defer dataMu.Unlock()
		data = make([]*T, 0, size)
		indices = make(map[string]bulkIndicies, indexLength)
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
		defer close(dataCh)
		timer := time.After(lazy)
		nameQueueLenMap := gq.Length()
		resetData(len(nameQueueLenMap))
		// add enqueueSignal because get enqueueSignal first
		gq.sendSignal()
		for {
			select {
			case <-timer:
				dataCh <- bulkQueueChData[T]{
					data:    data,
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
					go func(wg *sync.WaitGroup, name string, length int, queue *Queue[T]) {
						defer wg.Done()
						for i := 0; i < length; i++ {
							message, qErr := q.Dequeue()
							if qErr != nil {
								err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
							}
							appendData(message)
						}
					}(&queueWg, name, length, q)
				}
				queueWg.Wait()
			}
		}
	}()

	for d := range dataCh {
		if len(d.data) == 0 {
			continue
		}
		fErr := f(d.data)
		if fErr != nil {
			err = errors.Join(err, fErr)
		}
		for name, bulkIndex := range d.indices {
			q, gqErr := gq.getQueue(name)
			if gqErr != nil {
				err = errors.Join(err, gqErr)
			}
			wiErr := q.writeIndex(bulkIndex.page, bulkIndex.globalIndex, bulkIndex.localIndex)
			if wiErr != nil {
				err = errors.Join(err, wiErr)
			}
		}
	}
	return err
}

func (gq *GroupQueue[T]) lengthWithNotEmpty() map[string]int {
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	nameQueueLenMap := make(map[string]int, len(gq.queues))
	for name, q := range gq.queues {
		if ql := q.Length(); ql > 0 {
			nameQueueLenMap[name] = ql
		}
	}
	return nameQueueLenMap
}

func (gq *GroupQueue[T]) Length() map[string]int {
	nameQueueLenMap := make(map[string]int, len(gq.queues))
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
func (gq *GroupQueue[T]) CloseQueue() error {
	var err error
	gq.mu.Lock()
	defer gq.mu.Unlock()
	for _, q := range gq.queues {
		closeErr := q.CloseQueue()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	if err == nil {
		gq.closeSig <- struct{}{}
	}
	return err
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

// UpdateIndex updates the index of a given message in its corresponding queue.
//
// Parameters:
//   - message: The message whose index needs to be updated.
//
// Returns:
//   - error: An error if the update fails.
//
// Example:
//
//	err := gq.UpdateIndex(message)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (gq *GroupQueue[T]) UpdateIndex(message *Message[T]) error {
	var err error
	q, gqErr := gq.getQueue(message.name)
	if gqErr != nil {
		err = errors.Join(err, gqErr)
	}
	iErr := q.writeIndex(message.page, message.globalIndex, message.localIndex)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return err
}
