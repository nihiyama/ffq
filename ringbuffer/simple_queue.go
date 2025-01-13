// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ringbuffer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var (
	indexFilename = "index"
	queueFilename = "queue"
)

// Queue represents a file-based FIFO queue with a generic type T.
// It supports operations such as enqueue, dequeue, bulk enqueue/dequeue, and manages the queue across multiple pages.
type Queue[T any] struct {
	size            uint64 // The maximum number of items in the queue.
	mask            uint64
	tail            uint64
	head            uint64
	maxPage         uint64
	currentPage     uint64
	notEmpty        chan struct{}
	notFull         chan struct{}
	name            string                         // The name of the queue.
	fileDir         string                         // The directory where the queue files are stored.
	queueFile       atomic.Value                   // The file where the queue data is written.
	indexFile       *os.File                       // The file where the queue's index is stored.
	encoder         func(v any) ([]byte, error)    // Function to encode data before writing to the queue.
	decoder         func(data []byte, v any) error // Function to decode data when reading from the queue.
	queue           []*Message[T]
	isClose         bool
	initializeBlock chan struct{} // A channel to block until the queue is fully initialized.
}

// NewQueue creates a new Queue with the given name and options.
//
// Parameters:
//   - name: The name of the queue.
//   - opts: A list of options to customize the queue (e.g., queue size, max pages, file directory).
//
// Returns:
//   - *Queue: A pointer to the newly created Queue.
//   - error: An error if the queue initialization fails.
//
// Example:
//
//	q, err := NewQueue[string]("myQueue", WithQueueSize(100), WithMaxPages(5))
//	if err != nil {
//	    log.Fatal(err)
//	}
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

	queue := make([]*Message[T], size)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	index := readIndex(indexFilePath)

	var tail, head uint64
	if index == nil {
		tail = 0
		head = 0
	} else {
		tail = *index + 1
		head = *index + 1
	}

	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	q := Queue[T]{
		size:            size,
		mask:            size - 1,
		tail:            tail,
		head:            head,
		maxPage:         maxPage,
		currentPage:     0,
		notEmpty:        make(chan struct{}, 1),
		notFull:         make(chan struct{}, 1),
		name:            name,
		fileDir:         fileDir,
		indexFile:       indexFile,
		encoder:         encoder,
		decoder:         decoder,
		queue:           queue,
		isClose:         false,
		initializeBlock: make(chan struct{}),
	}
	q.signalNotFull()

	go func() {
		q.initialize()
	}()

	return &q, nil
}

func (q *Queue[T]) storeTail(tail uint64, nums uint64) {
	if tail+nums < q.size*q.maxPage {
		atomic.StoreUint64(&q.tail, tail+nums)
	} else {
		atomic.StoreUint64(&q.tail, tail+nums-(q.size*q.maxPage))
	}
}

func (q *Queue[T]) storeHead(head uint64, nums uint64) {
	if head+nums < q.size*q.maxPage {
		atomic.StoreUint64(&q.head, head+nums)
	} else {
		atomic.StoreUint64(&q.head, head+nums-(q.size*q.maxPage))
	}
}

func (q *Queue[T]) enqueue(tail uint64, item *T) error {
	buf, err := q.encoder([]*T{item})
	if err != nil {
		return err
	}

	m := &Message[T]{
		index: tail,
		name:  q.name,
		item:  item,
	}
	q.queue[tail&q.mask] = m

	err = q.writeQueue(buf, tail)
	if err != nil {
		return nil
	}
	return nil
}

func (q *Queue[T]) bulkEnqueue(tail uint64, items []*T) error {
	buf, err := q.encoder(items)
	if err != nil {
		return err
	}

	for i, item := range items {
		m := &Message[T]{
			index: tail + uint64(i),
			name:  q.name,
			item:  item,
		}
		q.queue[tail&q.mask] = m
		tail++
	}

	err = q.writeQueue(buf, tail)
	if err != nil {
		return nil
	}
	return nil
}

func (q *Queue[T]) dequeue(head uint64) *Message[T] {
	m := q.queue[head&q.mask]
	return m
}

// func (q *Queue[T]) bulkDequeue(head uint64, batch uint64) []*Message[T] {
// 	ms := make([]*Message[T], batch)
// 	if (head&q.mask)+batch > q.size {
// 		remainingCap := batch - (q.size - (head & q.mask))
// 		copy(ms, q.queue[head&q.mask:q.size])
// 		copy(ms, q.queue[0:remainingCap])
// 	} else {
// 		copy(ms, q.queue[head&q.mask:((head+batch-1)&q.mask)+1])
// 	}
// 	return ms
// }

// Enqueue adds a single item to the queue.
//
// Parameters:
//   - data: The data to be added to the queue.
//
// Returns:
//   - error: An error if the enqueue operation fails.
//
// Example:
//
//	data := Data{...}
//	err := q.Enqueue(&dataItem)
//	if err != nil {
//		log.Fatal(err)
//	}
func (q *Queue[T]) Enqueue(item *T) error {
	var err error
	for {
		tail := atomic.LoadUint64(&q.tail)
		head := atomic.LoadUint64(&q.head)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available < q.size {
			err = q.enqueue(tail, item)
			q.storeTail(tail, 1)
			q.signalNotEmpty()
			return err
		}
		<-q.notFull
	}
}

// BulkEnqueue adds multiple items to the queue in a single operation.
//
// Parameters:
//   - data: A slice of data items to be added to the queue.
//
// Returns:
//   - error: An error if the bulk enqueue operation fails.
//
// Example:
//
//	data := []*Data{{...},{...},...}
//	err := q.BulkEnqueue(data)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (q *Queue[T]) BulkEnqueue(items []*T) error {
	var err error
	itemLength := uint64(len(items))
	var itemIndex uint64
	var cap uint64
	for {
		tail := atomic.LoadUint64(&q.tail)
		head := atomic.LoadUint64(&q.head)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available < q.size {
			// constraints on file and ringbuffer integrity
			if (tail & q.mask) >= (head & q.mask) {
				// q.size - (tail & q.mask) = capacity from tail to the beginning
				cap = q.size - (tail & q.mask)
			} else {
				// (head & q.mask) - (tail & q.mask) = capacity from tail to head
				cap = (head & q.mask) - (tail & q.mask)
			}
			if itemLength < itemIndex+cap {
				cap = itemLength - itemIndex
			}
			err = q.bulkEnqueue(tail, items[itemIndex:itemIndex+cap])
			itemIndex = itemIndex + cap
			q.storeTail(tail, cap)
			q.signalNotEmpty()
			if itemIndex == itemLength {
				return err
			}
		} else {
			<-q.notFull
		}
	}
}

// Dequeue retrieves and returns a single message from the queue.
//
// Returns:
//   - *Message[T]: The dequeued message.
//   - error: An error if the dequeue operation fails or the queue is closed.
//
// Example:
//
//	m, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Dequeued message:", message)
func (q *Queue[T]) Dequeue() (*Message[T], error) {
	for {
		head := atomic.LoadUint64(&q.head)
		tail := atomic.LoadUint64(&q.tail)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available > 0 {
			m := q.dequeue(head)
			q.storeHead(head, 1)
			q.signalNotFull()
			return m, nil
		}

		if tail == head {
			if q.isClose {
				return nil, ErrQueueClose
			}
			<-q.notEmpty
		}
	}
}

// BulkDequeue retrieves multiple messages from the queue and returns them in a slice.
//
// Parameters:
//   - size: The maximum number of messages to dequeue in one operation.
//   - lazy: A duration to wait between dequeue operations.
//
// Returns:
//   - []*Message[T]: A slice of dequeued messages.
//   - error: An error if the bulk dequeue operation fails or the queue is closed.
//
// Example:
//
//	ms, err := q.BulkDequeue(10, 100*time.Millisecond)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Bulk dequeued messages:", ms)
func (q *Queue[T]) BulkDequeue(size uint64, lazy time.Duration) ([]*Message[T], error) {
	var err error
	ms := make([]*Message[T], 0, size)

	m, err := q.Dequeue()
	if IsErrQueueClose(err) {
		return nil, err
	}
	ms = append(ms, m)

	timer := time.After(lazy)
	for {
		head := atomic.LoadUint64(&q.head)
		tail := atomic.LoadUint64(&q.tail)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available > 0 {
			m := q.dequeue(head)
			q.storeHead(head, 1)
			ms = append(ms, m)
		}
		// batch := tail - head
		// if batch > 0 {
		// 	if batch > size {
		// 		batch = size
		// 	}
		// 	got := q.bulkDequeue(head, batch)
		// 	q.storeHead(head, batch)
		// 	q.signalNotFull()
		// 	ms = append(ms, got...)
		// 	size = size - batch
		// }

		if uint64(len(ms)) == size {
			q.signalNotFull()
			return ms, nil
		}
		if tail == head {
			// return ErrQueueClose at next time
			if q.isClose {
				return ms, nil
			}
			select {
			case <-timer:
				q.signalNotFull()
				return ms, nil
			case <-q.notEmpty:
			}
		}
		select {
		case <-timer:
			q.signalNotFull()
			return ms, nil
		default:
		}
	}
}

// FuncAfterDequeue applies a given function to the data of a dequeued item.
//
// Parameters:
//   - f: A function that processes the dequeued item.
//
// Returns:
//   - error: An error if the dequeue or function application fails.
//
// Example:
//
//	err := q.FuncAfterDequeue(func(data *T) error {
//	    fmt.Println("Processing item:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (q *Queue[T]) FuncAfterDequeue(f func(*T) error) error {
	var err error
	for {
		head := atomic.LoadUint64(&q.head)
		tail := atomic.LoadUint64(&q.tail)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available > 0 {
			m := q.dequeue(head)
			err = f(m.item)
			if err != nil {
				return err
			}
			err = q.writeIndex(m.index)
			if err != nil {
				return err
			}
			q.storeHead(head, 1)
			q.signalNotFull()
			return nil
		}

		if tail == head {
			if q.isClose {
				return ErrQueueClose
			}
			<-q.notEmpty
		}
	}
}

// FuncAfterBulkDequeue applies a given function to multiple dequeued items in a batch.
//
// Parameters:
//   - size: The maximum number of items to dequeue in one batch.
//   - lazy: A duration to wait between dequeue operations.
//   - f: A function that processes the batch of dequeued items.
//
// Returns:
//   - error: An error if the bulk dequeue or function application fails.
//
// Example:
//
//	err := q.FuncAfterBulkDequeue(10, 100*time.Millisecond, func(data []*T) error {
//	    fmt.Println("Processing batch:", data)
//	    return nil
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (q *Queue[T]) FuncAfterBulkDequeue(size uint64, lazy time.Duration, f func([]*T) error) error {
	var err error
	items := make([]*T, 0, size)
	var lastIndex uint64 = 0

	m, err := q.Dequeue()
	if IsErrQueueClose(err) {
		return err
	}
	items = append(items, m.item)
	lastIndex = m.index

	timer := time.After(lazy)
LOOP:
	for {
		head := atomic.LoadUint64(&q.head)
		tail := atomic.LoadUint64(&q.tail)

		available := tail - head
		if tail < head {
			available = tail + (q.size * q.maxPage) - head
		}
		if available > 0 {
			m := q.dequeue(head)
			q.storeHead(head, 1)
			items = append(items, m.item)
			lastIndex = m.index
		}
		if uint64(len(items)) == size {
			break LOOP
		}
		if tail == head {
			// return ErrQueueClose at next time
			if q.isClose {
				break LOOP
			}
			select {
			case <-timer:
				break LOOP
			case <-q.notEmpty:
			}
		}
		select {
		case <-timer:
			break LOOP
		default:
		}
	}
	err = f(items)
	if err != nil {
		return err
	}
	err = q.writeIndex(lastIndex)
	if err != nil {
		return err
	}
	q.signalNotFull()
	return err
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
	_, err = buf.Write([]byte{0x00A})
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
	newQueueFile, err := os.Create(newQueueFilepath)
	if err != nil {
		return err
	}
	q.queueFile.Store(newQueueFile)
	return nil
}

// UpdateIndex updates the index of a given message in the queue.
//
// Parameters:
//   - message: The message whose index needs to be updated.
//
// Returns:
//   - error: An error if the index update fails.
//
// Example:
//
//	err := q.UpdateIndex(message)
//	if err != nil {
//	    log.Fatal(err)
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

// Length returns the current number of items in the queue.
//
// Returns:
//   - int: The number of items in the queue.
//
// Example:
//
//	length := q.Length()
//	fmt.Println("Queue length:", length)
func (q *Queue[T]) Length() uint64 {
	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)
	if tail >= head {
		return tail - head
	} else {
		return tail + (q.size * q.maxPage) - head
	}
}

func (q *Queue[T]) initialize() {
	var queueFile *os.File

	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)
	startHead := head
	currentPage := head / q.size

	i := currentPage
	for {
		queueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, currentPage))
		stat, err := os.Stat(queueFilepath)
		if err != nil {
			if os.IsNotExist(err) {
				queueFile, err = os.Create(queueFilepath)
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
		queueFile, err = os.OpenFile(queueFilepath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("could not open file, %s, %v", queueFilepath, err))
		}
		scanner := bufio.NewScanner(queueFile)
		// TODO: scanner is cannnot read long data
		for scanner.Scan() {
			b := scanner.Bytes()
			var items []*T
			err = q.decoder(b, &items)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(b), err))
			}
			for j, item := range items {
				if uint64(j)+(i*q.size) < startHead {
					continue
				}
				tail = atomic.LoadUint64(&q.tail)
				head = atomic.LoadUint64(&q.head)

				m := &Message[T]{
					index: tail,
					name:  q.name,
					item:  item,
				}
				available := tail - head
				if tail < head {
					available = tail + (q.size * q.maxPage) - head
				}
				if available < q.size {
					q.queue[tail&q.mask] = m
					q.storeTail(tail, 1)
					q.signalNotEmpty()
				} else {
					<-q.notFull
				}
			}
		}
		if err := scanner.Err(); err != nil {
			panic(fmt.Sprintf("scan error, file: %s, %v", queueFilepath, err))
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
		i++
	}

	// release blocking
	q.initializeBlock <- struct{}{}
	close(q.initializeBlock)
}

// WaitInitialize blocks until the queue is fully initialized.
//
// Example:
//
//	q.WaitInitialize()
func (q *Queue[T]) WaitInitialize() {
	<-q.initializeBlock
}

// CloseQueue closes the queue and its associated file, signaling that no more data can be added.
//
// Returns:
//   - error: An error if the file closure fails.
//
// Example:
//
//	err := q.CloseQueue()
//	if err != nil {
//	    log.Fatal(err)
//	}
func (q *Queue[T]) CloseQueue() {
	q.isClose = true
	// last signal
	q.signalNotEmpty()
}

// CloseIndex closes the index file and releases associated resources.
//
// Returns:
//   - error: An error if the index file could not be closed.
//
// Example:
//
//	err := queue.CloseIndex()
//	if err != nil {
//	  log.Fatal(err)
//	}
func (q *Queue[T]) CloseIndex() error {
	err := q.indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue[T]) signalNotEmpty() {
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
}

func (q *Queue[T]) signalNotFull() {
	select {
	case q.notFull <- struct{}{}:
	default:
	}
}
