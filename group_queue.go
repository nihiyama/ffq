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

// GroupQueue represents a group of queues where each queue is associated with a unique name.
// It supports operations such as enqueuing, dequeuing, and bulk processing across multiple queues.
//
// Fields:
//   - name: The name of the group queue.
//   - fileDir: The directory where the queue files are stored.
//   - queueSize: The maximum number of items that can be held in each queue.
//   - maxPages: The number of files used in a single rotation cycle.//   - maxFileSize: The maximum size of a single queue file.
//   - maxIndexSize: The maximum size of the index file.
//   - initializeBlock: A channel to block until initialization is complete.
//   - queues: A map of queue names to their respective Queue instances.
//   - enqueueSig: A channel used for signaling between operations.
//   - mu: A mutex for synchronizing access to the queues.
type GroupQueue[T any] struct {
	queueSize       int
	maxPages        int
	name            string
	fileDir         string
	queues          map[string]*Queue[T]
	encoder         func(v any) ([]byte, error)
	decoder         func(data []byte, v any) error
	initializeBlock chan struct{}
	enqueueSig      chan struct{}
	closeSig        chan struct{}
	mu              *sync.RWMutex
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

// NewGroupQueue creates a new GroupQueue with the specified name and options.
//
// Parameters:
//   - name: The name of the group queue.
//   - opts: A variadic list of options to customize the group queue settings.
//
// Returns:
//   - *GroupQueue[T]: A pointer to the newly created GroupQueue instance.
//   - error: An error if the group queue could not be created.
//
// Example:
//
//	groupQueue, err := NewGroupQueue[Data]("myGroupQueue")
//	if err != nil {
//	  log.Fatal(err)
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

// addQueue adds a new Queue to the GroupQueue with the specified name.
//
// Parameters:
//   - name: The name of the new queue to be added.
//
// Returns:
//   - error: An error if the queue could not be added.
//
// Example:
//
//	err := groupQueue.addQueue("queue1")
//	if err != nil {
//	  log.Fatal(err)
//	}
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

// Enqueue adds a single item to the specified queue within the GroupQueue.
//
// Parameters:
//   - name: The name of the queue to which the item should be added.
//   - data: A pointer to the item to be added.
//
// Returns:
//   - error: An error if the item could not be enqueued.
//
// Example:
//
//	data := Data{...}
//	err := groupQueue.Enqueue("queue1", &data)
//	if err != nil {
//	  log.Fatal(err)
//	}
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

// BulkEnqueue adds multiple items to the specified queue within the GroupQueue.
//
// Parameters:
//   - name: The name of the queue to which the items should be added.
//   - data: A slice of pointers to the items to be added.
//
// Returns:
//   - error: An error if the items could not be enqueued.
//
// Example:
//
//	data := []*Data{{...},{...},...}
//	err := groupQueue.BulkEnqueue("queue1", data)
//	if err != nil {
//	  log.Fatal(err)
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

// Dequeue removes and returns items from the queues within the GroupQueue in a round-robin fashion.
//
// Parameters:
//   - batch: The number of items to dequeue from each queue in one operation.
//
// Returns:
//   - chan *Message[T]: A channel that yields dequeued messages wrapped in a Message struct.
//   - error: An error if there was an issue during the dequeue operation.
//
// Example:
//
//	messages, err := groupQueue.Dequeue(10)
//	if err != nil {
//	  log.Fatal(err)
//	}
//	for message := range messages {
//	  fmt.Println("Dequeued message:", *message.Data())
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

// BulkDequeue removes and returns multiple items from the queues within the GroupQueue.
//
// Parameters:
//   - batch: The number of items to dequeue from each queue in one operation.
//   - size: The total number of items to dequeue across all queues.
//   - lazy: The time in milliseconds to wait for more items before returning.
//
// Returns:
//   - chan []*Message[T]: A channel that yields slices of dequeued messages wrapped in Message structs.
//   - error: An error if there was an issue during the bulk dequeue operation.
//
// Example:
//
//	bulkMessages, err := groupQueue.BulkDequeue(10, 100, 500)
//	if err != nil {
//	  log.Fatal(err)
//	}
//	for messages := range bulkMessages {
//	  for _, message := range messages {
//	    fmt.Println("Dequeued message:", *message.Data())
//	  }
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

// FuncAfterDequeue applies a function to the data of each dequeued item and updates the index.
//
// Parameters:
//   - batch: The number of items to dequeue from each queue in one operation.
//   - f: A function that takes a pointer to the dequeued data and returns an error.
//
// Returns:
//   - error: An error if the function or index update fails.
//
// Example:
//
//	err := groupQueue.FuncAfterDequeue(10, func(data *Data) error {
//	  fmt.Println(*data)
//	  return nil
//	})
//	if err != nil {
//	  log.Fatal(err)
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

// FuncAfterBulkDequeue applies a function to the data of multiple dequeued items and updates the indices.
//
// Parameters:
//   - batch: The number of items to dequeue from each queue in one operation.
//   - size: The total number of items to dequeue across all queues.
//   - lazy: The time in milliseconds to wait for more items before applying the function.
//   - f: A function that takes a slice of pointers to the dequeued data and returns an error.
//
// Returns:
//   - error: An error if the function or index update fails.
//
// Example:
//
//	err := groupQueue.FuncAfterBulkDequeue(10, 100, 500, func(data []*Data) error {
//	  for _, d := range data {
//	    fmt.Println(*d)
//	  }
//	  return nil
//	})
//	if err != nil {
//	  log.Fatal(err)
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
	fmt.Println(gq.fileDir, entries)

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

// WaitInitialize blocks until the GroupQueue initialization is complete.
//
// Example:
//
//	groupQueue.WaitInitialize()
//	fmt.Println("GroupQueue initialized")
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

// CloseQueue closes all the queues in the GroupQueue.
//
// This method locks the GroupQueue, iterates over all the queues in it, and calls their
// respective CloseQueue methods. If any errors occur while closing the queues, they are
// accumulated and returned as a combined error.
//
// Returns:
//   - error: An error that accumulates any issues that occurred while closing the queues.
//     If all queues are closed successfully, nil is returned.
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

// CloseIndex closes the index files of all queues in the GroupQueue after ensuring all queues are empty.
//
// This method first checks if all queues in the GroupQueue are empty by repeatedly calling
// lengthWithNotEmpty and waiting for the specified interval if there are still items in any queue.
// Once all queues are confirmed to be empty, it closes the index files of each queue.
// If any errors occur while closing the index files, they are accumulated and returned as a combined error.
//
// Parameters:
//   - interval: The duration to wait between checks for non-empty queues.
//
// Returns:
//   - error: An error that accumulates any issues that occurred while closing the index files.
//     If all index files are closed successfully, nil is returned.
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

// UpdateIndex updates the index of the specified queue within the GroupQueue.
//
// This method locates the queue associated with the given message and attempts to update its index
// using the message's index value. If the specified queue is not found, or if there is an error
// during the index update, the errors are accumulated and returned.
//
// Parameters:
//   - message: A pointer to a Message containing the queue name and index to be updated.
//
// Returns:
//   - error: An error that combines any errors encountered during the update process.
//     If the queue is found and the index is updated successfully, nil is returned.
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
