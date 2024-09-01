// Package ffq provides a File-based FIFO Queue implementation that supports generic types.
package ffq

import (
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
//   - enqueueWriteSize: The number of items to write to disk in each batch.
//   - pageSize: The number of files used in a single rotation cycle.
//   - dataFixedLength: The fixed size of the data block written to each file.
//   - maxFileSize: The maximum size of a single queue file.
//   - maxIndexSize: The maximum size of the index file.
//   - initializeBlock: A channel to block until initialization is complete.
//   - queues: A map of queue names to their respective Queue instances.
//   - sig: A channel used for signaling between operations.
//   - mu: A mutex for synchronizing access to the queues.
type GroupQueue[T any] struct {
	name             string
	fileDir          string
	queueSize        int
	enqueueWriteSize int
	pageSize         int
	dataFixedLength  uint64
	maxFileSize      uint64
	maxIndexSize     int
	initializeBlock  chan struct{}
	queues           map[string]*Queue[T]
	sig              chan struct{}
	mu               *sync.RWMutex
}

type bulkQueueChData[T any] struct {
	data     []*T
	indicies map[string]int
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

	enqueueWriteSize := 10
	if options.enqueueWriteSize != nil {
		enqueueWriteSize = *options.enqueueWriteSize
	}

	pageSize := 2
	if options.pageSize != nil {
		pageSize = *options.pageSize
	}

	var dataFixedLength uint64 = 8
	if options.dataFixedLength != nil {
		dataFixedLength = *options.dataFixedLength
	}

	maxFileSize := uint64(queueSize) * dataFixedLength
	maxIndexSize := queueSize * pageSize
	initializeBlock := make(chan struct{})
	queues := make(map[string]*Queue[T], 10)
	var mu sync.RWMutex
	sig := make(chan struct{}, 1)

	gq := GroupQueue[T]{
		name:             name,
		fileDir:          fileDir,
		queueSize:        queueSize,
		enqueueWriteSize: enqueueWriteSize,
		pageSize:         pageSize,
		dataFixedLength:  dataFixedLength,
		maxFileSize:      maxFileSize,
		maxIndexSize:     maxIndexSize,
		initializeBlock:  initializeBlock,
		queues:           queues,
		mu:               &mu,
		sig:              sig,
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
		WithPageSize(gq.pageSize),
		WithQueueSize(gq.queueSize),
		WithEnqueueWriteSize(gq.enqueueWriteSize),
		WithDataFixedLength(gq.dataFixedLength),
	)
	if err != nil {
		return err
	}
	q.WaitInitialize()
	gq.mu.Lock()
	gq.queues[name] = q
	gq.mu.Unlock()
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
			time.Sleep(100 * time.Microsecond)
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
	<-gq.sig
	nameQueueLenMap := gq.lengthWithNotEmpty()
	go func() {
		defer close(mCh)
		var queueWg sync.WaitGroup
		for name, length := range nameQueueLenMap {
			q, gqErr := gq.getQueue(name)
			if gqErr != nil {
				err = errors.Join(err, gqErr)
			}
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
	var sliceMu sync.Mutex

	appendMessages := func(m *Message[T]) {
		sliceMu.Lock()
		messages = append(messages, m)
		if len(messages) == size {
			msCh <- messages
			messages = make([]*Message[T], 0, size)
		}
		sliceMu.Unlock()
	}
	resetMessages := func() {
		sliceMu.Lock()
		messages = make([]*Message[T], 0, size)
		sliceMu.Unlock()
	}
	<-gq.sig

	go func() {
		defer close(msCh)
		timer := time.After(lazy)
		resetMessages()
		// add signal because get signal first
		gq.sendSignal()
		for {
			select {
			case <-timer:
				msCh <- messages
				return
			case <-gq.sig:
				nameQueueLenMap := gq.lengthWithNotEmpty()
				var queueWg sync.WaitGroup
				for name, length := range nameQueueLenMap {
					q, gqErr := gq.getQueue(name)
					if gqErr != nil {
						err = errors.Join(err, gqErr)
					}
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
func (gq *GroupQueue[T]) FuncAfterDequeue(batch int, f func(*T) error) error {
	var err error

	<-gq.sig

	// check name and queue length
	nameQueueLenMap := gq.lengthWithNotEmpty()

	for len(nameQueueLenMap) > 0 {
		for name, length := range nameQueueLenMap {
			q, gqErr := gq.getQueue(name)
			if gqErr != nil {
				err = errors.Join(err, gqErr)
			}
			// dequeue nums at one time
			n := batch
			if n > length {
				n = length
			}
			for i := 0; i < n; i++ {
				message, qErr := q.Dequeue()
				if qErr != nil {
					err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
				}

				fErr := f(message.data)
				if fErr != nil {
					err = errors.Join(err, fErr)
				}
				iErr := q.writeIndex(message.index)
				if iErr != nil {
					err = errors.Join(err, iErr)
				}
			}
			length = length - n
			nameQueueLenMap[name] = length
			if length == 0 {
				delete(nameQueueLenMap, name)
			}
		}
	}
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
func (gq *GroupQueue[T]) FuncAfterBulkDequeue(batch int, size int, lazy time.Duration, f func([]*T) error) error {
	var err error
	dataCh := make(chan bulkQueueChData[T])
	<-gq.sig

	go func() {
		defer close(dataCh)
		timer := time.After(lazy)
		d := make([]*T, 0, size)
		indicies := make(map[string]int, len(gq.queues))
		// add signal because get signal first
		gq.sendSignal()
		for {
			select {
			case <-timer:
				dataCh <- bulkQueueChData[T]{
					data:     d,
					indicies: indicies,
				}
				return
			case <-gq.sig:
				nameQueueLenMap := gq.lengthWithNotEmpty()
				for len(nameQueueLenMap) > 0 {
					for name, length := range nameQueueLenMap {
						q, gqErr := gq.getQueue(name)
						if gqErr != nil {
							err = errors.Join(err, gqErr)
						}
						// dequeue nums at one time
						n := batch
						if n > length {
							n = length
						}

						for i := 0; i < n; i++ {
							message, qErr := q.Dequeue()
							indicies[name] = message.index
							if qErr != nil {
								err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, qErr))
							}
							d = append(d, message.data)
							if len(d) == size {
								dataCh <- bulkQueueChData[T]{
									data:     d,
									indicies: indicies,
								}
								d = make([]*T, 0, size)
								indicies = make(map[string]int, len(nameQueueLenMap))
							}
						}
						length = length - n
						nameQueueLenMap[name] = length
						if length == 0 {
							delete(nameQueueLenMap, name)
						}
					}
				}
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
		for name, index := range d.indicies {
			q, gqErr := gq.getQueue(name)
			if gqErr != nil {
				err = errors.Join(err, gqErr)
			}
			wiErr := q.writeIndex(index)
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
	case gq.sig <- struct{}{}:
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
	gq.mu.RLock()
	defer gq.mu.RUnlock()
	for _, q := range gq.queues {
		closeErr := q.CloseQueue()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
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
func (gq *GroupQueue[T]) CloseIndex(interval time.Duration) error {
	var err error
	for {
		gq.mu.RLock()
		nameQueueLenMap := gq.lengthWithNotEmpty()
		if len(nameQueueLenMap) == 0 {
			break
		}
		time.Sleep(interval)
	}
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
	iErr := q.writeIndex(message.index)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return err
}
