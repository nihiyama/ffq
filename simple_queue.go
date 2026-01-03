// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	indexFilename = "index"
	queueFilename = "queue"
	queueFileDir  = "/tmp/ffq"
)

type QueueType string

const (
	// SPSC indicates Single Producer Single Consumer mode.
	SPSC QueueType = "SPSC"
	// MPSC indicates Multiple Producer Single Consumer mode.
	MPSC QueueType = "MPSC"
)

// Queue represents a file-based FIFO queue with a generic type T.
// It supports operations such as enqueue, dequeue, bulk enqueue/dequeue, and manages the queue across multiple pages.
type Queue[T any] struct {
	size                  uint64                         // The maximum number of items in the queue.
	tail                  uint64                         // Current write index
	maxPage               uint64                         // Total number of pages available
	currentPage           uint64                         // Current page number used for writing
	name                  string                         // The name of the queue.
	fileDir               string                         // The directory where the queue files are stored.
	queueFile             atomic.Value                   // The file where the queue data is written.
	indexFile             *os.File                       // The file where the queue's index is stored.
	encoder               func(v any) ([]byte, error)    // Function to encode data before writing to the queue.
	decoder               func(data []byte, v any) error // Function to decode data when reading from the queue.
	enqueuer              func(item *T) error            // Internal function for enqueuing a single item
	bulkEnqueuer          func(items []*T) error         // Internal function for bulk enqueuing multiple items
	queue                 chan *Message[T]               // Internal message channel
	isQueueClosed         atomic.Bool                    // Flag indicating if the queue is closed
	isQueueClosedRecieved atomic.Bool                    // Flag indicating if a closed queue has been read from
	isIndexClosed         atomic.Bool                    // Flag indicating if the index file is closed
	initializeBlock       chan struct{}                  // A channel to block until the queue is fully initialized.
	mu                    sync.Mutex                     // Mutex for synchronizing multi-producer operations
}

// NewQueue creates a new Queue with the specified name and options.
//
// Parameters:
//   - name: The identifier for the queue. This is used to distinguish multiple queues.
//   - opts: A list of options to customize the queue (e.g., WithQueueSize, WithMaxPages, WithFileDir, WithEncoder, WithDecoder).
//
// Returns:
//   - *Queue[T]: A pointer to the newly created and partially initialized Queue.
//   - error: An error if the queue initialization fails (for example, if creating the file directory fails).
//
// Example:
//
//	// Create a queue with a buffer size of 100 items and up to 5 file pages.
//	q, err := ffq.NewQueue[string]("myQueue", ffq.WithQueueSize(100), ffq.WithMaxPages(5))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
//	// Wait for the background initialization to comple
func NewQueue[T any](name string, opts ...Option) (*Queue[T], error) {
	var err error

	// check options and set default settings
	var options options
	for _, opt := range opts {
		err := opt(&options)
		if err != nil {
			return nil, err
		}
	}

	var fileDir = queueFileDir
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

	var encoder func(v any) ([]byte, error) = json.Marshal
	if options.encoder != nil {
		encoder = *options.encoder
	}

	var decoder func(data []byte, v any) error = json.Unmarshal
	if options.decoder != nil {
		decoder = *options.decoder
	}

	queue := make(chan *Message[T], size)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	index := readIndex(indexFilePath)

	var tail uint64 = 0
	if index != nil {
		tail = *index + 1
	}

	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	q := Queue[T]{
		size:            size,
		tail:            tail,
		maxPage:         maxPage,
		currentPage:     0,
		name:            name,
		fileDir:         fileDir,
		indexFile:       indexFile,
		encoder:         encoder,
		decoder:         decoder,
		queue:           queue,
		initializeBlock: make(chan struct{}),
	}

	switch queueType {
	case SPSC:
		q.enqueuer = q.spEnqueue
		q.bulkEnqueuer = q.spBulkEnqueue
	case MPSC:
		q.enqueuer = q.mpEnqueue
		q.bulkEnqueuer = q.mpBulkEnqueue
	}

	q.isQueueClosed.Store(false)
	q.isQueueClosedRecieved.Store(false)
	q.isIndexClosed.Store(false)

	go func() {
		q.initialize()
	}()

	return &q, nil
}

func (q *Queue[T]) spStoreTail(tail uint64, nums uint64) uint64 {
	if tail+nums < q.size*q.maxPage {
		return atomic.AddUint64(&q.tail, nums)
	} else {
		return atomic.AddUint64(&q.tail, nums-(q.size*q.maxPage))
	}
}

func (q *Queue[T]) mpStoreTail(nums uint64) {
	if q.tail+nums < q.size*q.maxPage {
		q.tail += nums
	} else {
		q.tail += (nums - (q.size * q.maxPage))
	}
}

func (q *Queue[T]) enqueue(tail uint64, item *T) error {
	buf, err := q.encoder([]*T{item})
	if err != nil {
		return err
	}
	err = q.writeQueue(buf, tail)
	if err != nil {
		return err
	}

	m := &Message[T]{
		index: tail,
		name:  q.name,
		item:  item,
	}
	q.queue <- m

	return nil
}

func (q *Queue[T]) bulkEnqueue(tail uint64, items []*T) error {
	buf, err := q.encoder(items)
	if err != nil {
		return err
	}
	err = q.writeQueue(buf, tail)
	if err != nil {
		return err
	}

	for _, item := range items {
		m := &Message[T]{
			index: tail,
			name:  q.name,
			item:  item,
		}
		q.queue <- m
		tail++
	}
	return nil
}

func (q *Queue[T]) spEnqueue(item *T) error {
	var err error

	tail := atomic.LoadUint64(&q.tail)
	err = q.enqueue(tail, item)
	if err != nil {
		return err
	}
	q.spStoreTail(tail, 1)

	return err
}

func (q *Queue[T]) mpEnqueue(item *T) error {
	var err error
	q.mu.Lock()
	defer q.mu.Unlock()

	err = q.enqueue(q.tail, item)
	if err != nil {
		return err
	}
	q.mpStoreTail(1)

	return err
}

// Enqueue adds a single item to the queue.
//
// Parameters:
//   - item: A pointer to the data item to be enqueued.
//     For example: &dataItem
//
// Returns:
//   - error: An error if the enqueue operation fails.
//
// Example:
//
//	data := Data{...}
//	if err := q.Enqueue(&data); err != nil {
//	    log.Fatalf("Enqueue failed: %v", err)
//	}
func (q *Queue[T]) Enqueue(item *T) error {
	return q.enqueuer(item)
}

func (q *Queue[T]) spBulkEnqueue(items []*T) error {
	var err error

	itemLength := uint64(len(items))
	var itemIndex uint64
	tail := atomic.LoadUint64(&q.tail)

	for itemIndex < itemLength {
		batch := q.size - (tail % q.size)
		if itemIndex+batch > itemLength {
			batch = itemLength - itemIndex
		}
		err = q.bulkEnqueue(tail, items[itemIndex:itemIndex+batch])
		if err != nil {
			return err
		}
		itemIndex += batch
		tail = q.spStoreTail(tail, batch)
	}
	return nil
}

func (q *Queue[T]) mpBulkEnqueue(items []*T) error {
	var err error
	q.mu.Lock()
	defer q.mu.Unlock()

	itemLength := uint64(len(items))
	var itemIndex uint64

	for itemIndex < itemLength {
		batch := q.size - (q.tail % q.size)
		if itemIndex+batch > itemLength {
			batch = itemLength - itemIndex
		}
		err = q.bulkEnqueue(q.tail, items[itemIndex:itemIndex+batch])
		if err != nil {
			return err
		}
		itemIndex += batch
		q.mpStoreTail(batch)
	}
	return nil
}

// BulkEnqueue adds multiple items to the queue in a single operation.
//
// Parameters:
//   - items: A slice of pointers to data items to be enqueued.
//     For example: []*Data{&data1, &data2, ...}
//
// Returns:
//   - error: An error if the bulk enqueue operation fails.
//
// Example:
//
//	dataItems := []*Data{
//	    { ... },
//	    { ... },
//	}
//	if err := q.BulkEnqueue(dataItems); err != nil {
//	    log.Fatalf("BulkEnqueue failed: %v", err)
//	}
func (q *Queue[T]) BulkEnqueue(items []*T) error {
	return q.bulkEnqueuer(items)
}

// Dequeue retrieves and returns a single message from the queue.
//
// Returns:
//   - *Message[T]: The dequeued message containing the data item and its associated index.
//   - error: An error if the dequeue operation fails or if the queue is closed.
//
// Example:
//
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatalf("Dequeue failed: %v", err)
//	}
//	fmt.Printf("Dequeued message: %+v\n", msg)
func (q *Queue[T]) Dequeue() (*Message[T], error) {
	m, ok := <-q.queue
	if !ok {
		q.isQueueClosedRecieved.Store(true)
		return nil, ErrQueueClose
	}
	return m, nil
}

// BulkDequeue retrieves multiple messages from the queue in a single operation.
// It waits for either the specified number of messages or until the lazy duration expires.
//
// Parameters:
//   - size: The maximum number of messages to dequeue at once.
//   - lazy: The duration to wait between dequeue operations before returning the collected messages.
//
// Returns:
//   - []*Message[T]: A slice of dequeued messages.
//   - error: An error if the dequeue operation fails or if the queue is closed.
//
// Example:
//
//	messages, err := q.BulkDequeue(10, 100*time.Millisecond)
//	if err != nil {
//	    log.Fatalf("BulkDequeue failed: %v", err)
//	}
//	fmt.Printf("Bulk dequeued messages: %+v\n", messages)
func (q *Queue[T]) BulkDequeue(size uint64, lazy time.Duration) ([]*Message[T], error) {
	var err error
	ms := make([]*Message[T], 0, size)

	m, err := q.Dequeue()
	if err != nil {
		return nil, err
	}
	ms = append(ms, m)

	timer := time.After(lazy)
	for {
		select {
		case <-timer:
			return ms, nil
		case m, ok := <-q.queue:
			if !ok {
				// return ErrQueueClose at next time
				return ms, nil
			}
			ms = append(ms, m)
			if uint64(len(ms)) == size {
				return ms, nil
			}
		}
	}
}

// FuncAfterDequeue retrieves a single item from the queue and applies the specified function to it.
// After processing, the function updates the index file accordingly.
//
// Parameters:
//   - f: A function that processes the dequeued item. The function should return an error if processing fails.
//
// Returns:
//   - error: An error if either the dequeue operation or the function application (or index update) fails.
//
// Example:
//
//	err := q.FuncAfterDequeue(func(data *Data) error {
//	    fmt.Printf("Processing item: %+v\n", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatalf("FuncAfterDequeue failed: %v", err)
//	}
func (q *Queue[T]) FuncAfterDequeue(f func(*T) error) error {
	m, err := q.Dequeue()
	if err != nil {
		return err
	}

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

// FuncAfterBulkDequeue retrieves multiple items from the queue as a batch,
// applies the specified function to the batch, and then updates the index file based on the last item processed.
//
// Parameters:
//   - size: The maximum number of items to dequeue in the batch.
//   - lazy: The duration to wait between dequeue operations before processing the batch.
//   - f: A function that processes the batch of dequeued items. The function should return an error if processing fails.
//
// Returns:
//   - int: The actual number of items processed.
//   - error: An error if either the batch dequeue operation, the function application, or the index update fails.
//
// Example:
//
//	count, err := q.FuncAfterBulkDequeue(10, 100*time.Millisecond, func(data []*Data) error {
//	    fmt.Printf("Processing batch of %d items\n", len(data))
//	    return nil
//	})
//	if err != nil {
//	    log.Fatalf("FuncAfterBulkDequeue failed: %v", err)
//	}
//	fmt.Printf("Processed %d items\n", count)
func (q *Queue[T]) FuncAfterBulkDequeue(size uint64, lazy time.Duration, f func([]*T) error) (int, error) {
	var err error
	items := make([]*T, 0, size)

	m, err := q.Dequeue()
	if err != nil {
		return 0, err
	}
	lastM := m
	items = append(items, m.item)

	timer := time.After(lazy)
LOOP:
	for {
		select {
		case <-timer:
			break LOOP
		case m, ok := <-q.queue:
			if !ok {
				// return ErrQueueClose at next time
				break LOOP
			}
			items = append(items, m.item)
			lastM = m
			if uint64(len(items)) == size {
				break LOOP
			}
		}
	}
	fErr := f(items)
	if fErr != nil {
		err = errors.Join(err, fErr)
	}
	iErr := q.writeIndex(lastM.index)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return len(items), err
}

func (q *Queue[T]) writeQueue(b []byte, tail uint64) error {
	var err error

	// file rotation
	currentPage := atomic.LoadUint64(&q.currentPage)
	if (tail / q.size) != currentPage {
		err = q.rotateFile()
		if err != nil {
			return err
		}
	}

	buf := queueBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Grow(len(b) + 1)
	defer queueBufPool.Put(buf)

	_, err = buf.Write(b)
	if err != nil {
		return err
	}
	// add LF
	_, err = buf.Write([]byte{'\n'})
	if err != nil {
		return err
	}

	queueFile := q.queueFile.Load().(*os.File)
	_, err = buf.WriteTo(queueFile)
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue[T]) rotateFile() error {
	queueFile := q.queueFile.Load().(*os.File)
	queueFile.Close()
	currentPage := atomic.LoadUint64(&q.currentPage)
	currentPage++
	if currentPage == q.maxPage {
		currentPage = 0
	}
	atomic.StoreUint64(&q.currentPage, currentPage)
	newQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, currentPage))
	newQueueFile, err := os.OpenFile(newQueueFilepath, fCreateFlag, 0644)
	if err != nil {
		return err
	}
	q.queueFile.Store(newQueueFile)
	return nil
}

// UpdateIndex updates the index information of the specified message in the index file.
// This is used to record the position up to which the queue has been processed.
//
// Parameters:
//   - m: The message whose index is to be updated.
//     The index value contained in the message will be written to the index file.
//
// Returns:
//   - error: An error if updating the index fails.
//
// Example:
//
//	if err := q.UpdateIndex(message); err != nil {
//	    log.Fatalf("UpdateIndex failed: %v", err)
//	}
func (q *Queue[T]) UpdateIndex(m *Message[T]) error {
	return q.writeIndex(m.index)
}

func (q *Queue[T]) writeIndex(index uint64) error {
	var err error
	_, err = q.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	buf := indexBufPool.Get().(*[8]byte)

	// uint64 size is 8
	// | -- index(8) -- |
	binary.LittleEndian.PutUint64((*buf)[0:8], index)

	defer indexBufPool.Put(buf)

	_, err = q.indexFile.Write((*buf)[:])
	if err != nil {
		return err
	}
	return nil
}

// Length returns the current number of messages stored in the internal buffer of the queue.
// This reflects the number of unprocessed messages.
//
// Returns:
//   - uint64: The number of messages currently in the queue.
//
// Example:
//
//	currentLength := q.Length()
//	fmt.Printf("Current queue length: %d\n", currentLength)
func (q *Queue[T]) Length() uint64 {
	return uint64(len(q.queue))
}

func countJSONArrayItems(b []byte) (uint64, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil {
		return 0, err
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '[' {
		return 0, fmt.Errorf("expected JSON array")
	}

	var n uint64
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return 0, err
		}
		n++
	}

	if _, err := dec.Token(); err != nil {
		return 0, err
	}
	return n, nil
}

func (q *Queue[T]) initialize() {
	var queueFile *os.File

	tail := atomic.LoadUint64(&q.tail)
	startTail := tail
	currentPage := tail / q.size

	for {
		queueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, currentPage))
		stat, err := os.Stat(queueFilepath)
		if err != nil {
			if os.IsNotExist(err) {
				queueFile, err = os.OpenFile(queueFilepath, fCreateFlag, 0644)
				if err != nil {
					panic(fmt.Sprintf("could not create file, %s, %v", queueFilepath, err))
				}
				atomic.StoreUint64(&q.currentPage, currentPage)
				q.queueFile.Store(queueFile)
				break
			} else {
				panic(err)
			}
		}

		// read queue file and set queue
		queueFile, err = os.OpenFile(queueFilepath, fOpenFlag, 0644)
		if err != nil {
			panic(fmt.Sprintf("could not open file, %s, %v", queueFilepath, err))
		}
		reader := bufio.NewReader(queueFile)
		var itemNums uint64 = 0
		for {
			b, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					if len(b) == 0 {
						break
					}
				} else {
					panic(fmt.Sprintf("could not read file, %v", err))
				}
			}
			batchStart := itemNums + (currentPage * q.size)
			if batchStart < startTail {
				count, err := countJSONArrayItems(b)
				if err != nil {
					panic(fmt.Sprintf("could not count items, %s, %v", string(b), err))
				}
				if batchStart+count <= startTail {
					itemNums += count
					continue
				}
			}
			var items []*T
			err = q.decoder(b, &items)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(b), err))
			}
			for _, item := range items {
				if uint64(itemNums)+(currentPage*q.size) < startTail {
					itemNums++
					continue
				}
				m := &Message[T]{
					index: tail,
					name:  q.name,
					item:  item,
				}
				q.queue <- m
				tail = q.spStoreTail(tail, 1)
				if tail == 0 {
					// reset 0 page
					startTail = 0
				}
				itemNums++
			}
		}

		// check next page
		if (tail / q.size) == currentPage {
			atomic.StoreUint64(&q.currentPage, currentPage)
			q.queueFile.Store(queueFile)
			break
		}

		currentPage++
		if currentPage == q.maxPage {
			currentPage = 0
		}
		nextQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, currentPage))
		nextStat, err := os.Stat(nextQueueFilepath)
		if err != nil {
			if !os.IsNotExist(err) {
				panic(err)
			}
		} else {
			if stat.ModTime().After(nextStat.ModTime()) {
				os.Remove(nextQueueFilepath)
			}
		}
	}

	// release blocking
	q.initializeBlock <- struct{}{}
	close(q.initializeBlock)
}

// WaitInitialize blocks until the queue has completed its background initialization process.
// Since the queue may load existing data from files and perform recovery in the background,
// this method should be called to ensure the queue is fully initialized before starting operations.
//
// Example:
//
//	// Immediately after creating the queue, wait for initialization to complete.
//	q.WaitInitialize()
//	fmt.Println("Queue initialization complete")
func (q *Queue[T]) WaitInitialize() {
	<-q.initializeBlock
}

// CloseQueue closes the queue, disallowing any further enqueues,
// and closes the file associated with the queue data.
// After calling CloseQueue, further attempts to enqueue will result in errors.
//
// Returns:
//   - error: An error if closing the queue or its associated file fails.
//
// Example:
//
//	if err := q.CloseQueue(); err != nil {
//	    log.Fatalf("CloseQueue failed: %v", err)
//	}
func (q *Queue[T]) CloseQueue() error {
	close(q.queue)
	q.isQueueClosed.Store(true)
	queueFile := q.queueFile.Load().(*os.File)
	err := queueFile.Close()
	if err != nil {
		return err
	}
	return nil
}

// CloseIndex closes the index file and releases its associated resources.
// This should be called when the queue is no longer needed to ensure that all file handles are properly released.
//
// Returns:
//   - error: An error if closing the index file fails.
//
// Example:
//
//	if err := q.CloseIndex(); err != nil {
//	    log.Fatalf("CloseIndex failed: %v", err)
//	}
func (q *Queue[T]) CloseIndex() error {
	q.isIndexClosed.Store(true)
	err := q.indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}
