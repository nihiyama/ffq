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
	queueSize        uint64
	enqueueWriteSize int
	pageSize         int
	dataFixedLength  uint64
	maxFileSize      uint64
	maxIndexSize     uint64
	initializeBlock  chan struct{}
	queues           map[string]*Queue[T]
	sig              chan struct{}
	mu               *sync.Mutex
}

type bulkQueueChData[T any] struct {
	data     []*T
	indicies map[string]uint64
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

	var queueSize uint64 = 100
	if options.queueSize != nil {
		queueSize = *options.queueSize
	}

	var enqueueWriteSize int = 10
	if options.enqueueWriteSize != nil {
		enqueueWriteSize = *options.enqueueWriteSize
	}

	var pageSize int = 2
	if options.pageSize != nil {
		pageSize = *options.pageSize
	}

	var dataFixedLength uint64 = 8 * 1024
	if options.dataFixedLength != nil {
		dataFixedLength = *options.dataFixedLength * 1024
	}

	maxFileSize := queueSize * dataFixedLength
	maxIndexSize := queueSize * uint64(pageSize)

	gq := GroupQueue[T]{
		name:             name,
		fileDir:          fileDir,
		queueSize:        queueSize,
		enqueueWriteSize: enqueueWriteSize,
		pageSize:         pageSize,
		dataFixedLength:  dataFixedLength,
		maxFileSize:      maxFileSize,
		maxIndexSize:     maxIndexSize,
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
	q, ok := gq.queues[name]
	if !ok {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q = gq.queues[name]
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
	q, ok := gq.queues[name]
	if !ok {
		err = gq.addQueue(name)
		if err != nil {
			return err
		}
		q = gq.queues[name]
	}
	err = q.BulkEnqueue(data)
	if err != nil {
		return err
	}
	gq.sendSignal()
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
func (gq *GroupQueue[T]) Dequeue(batch int) (chan *Message[T], error) {
	var err error
	mCh := make(chan *Message[T])

	<-gq.sig

	gq.mu.Lock()
	nameQueueLenMap := make(map[string]int, len(gq.queues))
	for name, q := range gq.queues {
		nameQueueLenMap[name] = q.Length()
	}
	gq.mu.Unlock()
	go func() {
		defer close(mCh)
		for len(nameQueueLenMap) > 0 {
			for name, length := range nameQueueLenMap {
				q, ok := gq.queues[name]
				if !ok {
					err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound))
				}
				// dequeue nums at one time
				n := batch
				if n > length {
					n = length
				}
				for i := 0; i < n; i++ {
					message, qErr := q.Dequeue()
					if qErr != nil {
						err = errors.Join(err, qErr)
					}
					mCh <- message
				}
				length = length - n
				nameQueueLenMap[name] = length
				if length == 0 {
					delete(nameQueueLenMap, name)
				}
			}
		}
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
func (gq *GroupQueue[T]) BulkDequeue(batch int, size int, lazy int) (chan []*Message[T], error) {
	var err error
	msCh := make(chan []*Message[T])
	<-gq.sig

	go func() {
		defer close(msCh)
		timer := time.After(time.Duration(lazy) * time.Millisecond)
		// add signal becase get signal first
		gq.sendSignal()
		for {
			messages := make([]*Message[T], 0, size)
			select {
			case <-timer:
				msCh <- messages
				return
			case <-gq.sig:
				gq.mu.Lock()
				nameQueueLenMap := make(map[string]int, len(gq.queues))
				for name, q := range gq.queues {
					nameQueueLenMap[name] = q.Length()
				}
				gq.mu.Unlock()
				for len(nameQueueLenMap) > 0 {
					for name, length := range nameQueueLenMap {
						q, ok := gq.queues[name]
						if !ok {
							err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound))
						}
						// dequeue nums at one time
						n := batch
						if n > length {
							n = length
						}
						for i := 0; i < n; i++ {
							message, qErr := q.Dequeue()
							if qErr != nil {
								err = errors.Join(err, qErr)
							}
							messages = append(messages, message)
							if len(messages) == size {
								msCh <- messages
								messages = make([]*Message[T], 0, size)
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
	gq.mu.Lock()
	nameQueueLenMap := make(map[string]int, len(gq.queues))
	for name, q := range gq.queues {
		if ql := q.Length(); ql > 0 {
			nameQueueLenMap[name] = ql
		}
	}
	gq.mu.Unlock()

	for len(nameQueueLenMap) > 0 {
		for name, length := range nameQueueLenMap {
			q, ok := gq.queues[name]
			if !ok {
				err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound))
			}
			// dequeue nums at one time
			n := batch
			if n > length {
				n = length
			}
			for i := 0; i < n; i++ {
				message, qErr := q.Dequeue()
				if qErr != nil {
					err = errors.Join(err, qErr)
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
func (gq *GroupQueue[T]) FuncAfterBulkDequeue(batch int, size int, lazy int, f func([]*T) error) error {
	var err error
	dataCh := make(chan bulkQueueChData[T])
	<-gq.sig

	go func() {
		defer close(dataCh)
		timer := time.After(time.Duration(lazy) * time.Millisecond)
		// add signal becase get signal first
		gq.sendSignal()
		for {
			d := make([]*T, 0, size)
			indicies := make(map[string]uint64, len(gq.queues))
			select {
			case <-timer:
				dataCh <- bulkQueueChData[T]{
					data:     d,
					indicies: indicies,
				}
				return
			case <-gq.sig:
				gq.mu.Lock()
				nameQueueLenMap := make(map[string]int, len(gq.queues))
				for name, q := range gq.queues {
					nameQueueLenMap[name] = q.Length()
				}
				gq.mu.Unlock()
				for len(nameQueueLenMap) > 0 {
					for name, length := range nameQueueLenMap {
						q, ok := gq.queues[name]
						if !ok {
							err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound))
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
								err = errors.Join(err, qErr)
							}
							d = append(d, message.data)
							if len(d) == size {
								dataCh <- bulkQueueChData[T]{
									data:     d,
									indicies: indicies,
								}
								d = make([]*T, 0, size)
								indicies = make(map[string]uint64, len(nameQueueLenMap))
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
			q, ok := gq.queues[name]
			if !ok {
				err = errors.Join(err, fmt.Errorf("queue name: %s, %v", name, ErrQueueNotFound))
			}
			wiErr := q.writeIndex(index)
			if wiErr != nil {
				err = errors.Join(err, wiErr)
			}
		}
	}
	return err
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
