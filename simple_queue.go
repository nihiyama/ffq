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
	"time"
)

var (
	indexFilename = "index"
	queueFilename = "queue"
)

// Queue represents a file-based FIFO queue with a generic type T.
// It supports operations such as enqueue, dequeue, bulk enqueue/dequeue, and manages the queue across multiple pages.
type Queue[T any] struct {
	queueSize       int                            // The maximum number of items in the queue.
	maxPages        int                            // The maximum number of pages allowed for the queue.
	currentPage     int                            // The current page being written to.
	headGlobalIndex int                            // The global index of the head of the queue.
	name            string                         // The name of the queue.
	fileDir         string                         // The directory where the queue files are stored.
	queue           chan *Message[T]               // The queue channel for holding messages.
	queueFile       *os.File                       // The file where the queue data is written.
	indexFile       *os.File                       // The file where the queue's index is stored.
	encoder         func(v any) ([]byte, error)    // Function to encode data before writing to the queue.
	decoder         func(data []byte, v any) error // Function to decode data when reading from the queue.
	initializeBlock chan struct{}                  // A channel to block until the queue is fully initialized.
	qMu             *sync.Mutex                    // A mutex for queue operations.
	iMu             *sync.Mutex                    // A mutex for index operations.
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

	queue := make(chan *Message[T], queueSize)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	currentPage, tailGlobalIndex, tailLocalIndex, err := readIndex(indexFilePath)
	if err != nil {
		return nil, err
	}

	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	initializeBlock := make(chan struct{})
	var qMu sync.Mutex
	var iMu sync.Mutex

	q := Queue[T]{
		name:            name,
		fileDir:         fileDir,
		queueSize:       queueSize,
		maxPages:        maxPages,
		currentPage:     currentPage,
		queue:           queue,
		indexFile:       indexFile,
		encoder:         encoder,
		decoder:         decoder,
		initializeBlock: initializeBlock,
		qMu:             &qMu,
		iMu:             &iMu,
	}

	go func() {
		q.initialize(tailGlobalIndex, tailLocalIndex)
	}()

	return &q, nil
}

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
func (q *Queue[T]) Enqueue(data *T) error {
	q.qMu.Lock()
	defer q.qMu.Unlock()
	// write queue file
	var err error

	buf, err := q.encoder([]*T{data})
	if err != nil {
		return err
	}

	// write queue channel
	q.queue <- &Message[T]{
		name:        q.name,
		page:        q.currentPage,
		globalIndex: q.headGlobalIndex,
		localIndex:  0,
		data:        data,
	}

	q.headGlobalIndex++

	err = q.writeQueue(buf)
	if err != nil {
		return err
	}
	return err
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
func (q *Queue[T]) BulkEnqueue(data []*T) error {
	q.qMu.Lock()
	defer q.qMu.Unlock()

	var err error

	buf, err := q.encoder(data)
	if err != nil {
		return err
	}

	for i := 0; i < len(data); i++ {
		q.queue <- &Message[T]{
			name:        q.name,
			page:        q.currentPage,
			globalIndex: q.headGlobalIndex,
			localIndex:  i,
			data:        data[i],
		}
	}

	q.headGlobalIndex++

	err = q.writeQueue(buf)
	if err != nil {
		return err
	}
	return nil
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
	m, ok := <-q.queue
	if !ok {
		return nil, ErrQueueClose
	}
	return m, nil
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
func (q *Queue[T]) BulkDequeue(size int, lazy time.Duration) ([]*Message[T], error) {
	messages := make([]*Message[T], 0, size)
	m, ok := <-q.queue
	if !ok {
		return messages, ErrQueueClose
	}
	messages = append(messages, m)
	timer := time.After(lazy)
	for {
		select {
		case <-timer:
			return messages, nil
		case m, ok := <-q.queue:
			if !ok {
				return messages, ErrQueueClose
			}
			messages = append(messages, m)
			if len(messages) == size {
				return messages, nil
			}
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

	message, ok := <-q.queue
	if !ok {
		return ErrQueueClose
	}

	err = f(message.data)
	if err != nil {
		return err
	}

	err = q.writeIndex(message.page, message.globalIndex, message.localIndex)
	if err != nil {
		return err
	}
	return nil
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
func (q *Queue[T]) FuncAfterBulkDequeue(size int, lazy time.Duration, f func([]*T) error) error {
	var err error

	data := make([]*T, 0, size)
	m, ok := <-q.queue
	if !ok {
		return ErrQueueClose
	}
	page := m.page
	globalIndex := m.globalIndex
	localIndex := m.localIndex
	data = append(data, m.data)
	timer := time.After(lazy)
LOOP:
	for {
		select {
		case <-timer:
			break LOOP
		case m, ok := <-q.queue:
			if !ok {
				err = errors.Join(err, ErrQueueClose)
				return err
			}
			data = append(data, m.data)
			page = m.page
			globalIndex = m.globalIndex
			localIndex = m.localIndex
			if len(data) == size {
				break LOOP
			}
		}
	}
	fErr := f(data)
	if fErr != nil {
		err = errors.Join(err, fErr)
	}
	wiErr := q.writeIndex(page, globalIndex, localIndex)
	if err != nil {
		err = errors.Join(err, wiErr)
	}
	return err
}

func (q *Queue[T]) writeQueue(b []byte) error {
	var err error

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

	_, err = buf.WriteTo(q.queueFile)
	if err != nil {
		return err
	}

	if q.headGlobalIndex == q.queueSize {
		q.headGlobalIndex = 0
		err = q.rotateFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue[T]) rotateFile() error {
	q.queueFile.Close()
	q.currentPage++
	if q.currentPage == q.maxPages {
		q.currentPage = 0
	}
	newQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))
	newQueueFile, err := os.Create(newQueueFilepath)
	if err != nil {
		return err
	}
	q.queueFile = newQueueFile
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
func (q *Queue[T]) UpdateIndex(message *Message[T]) error {
	return q.writeIndex(message.page, message.globalIndex, message.localIndex)
}

func (q *Queue[T]) writeIndex(page int, globalIndex int, localIndex int) error {
	var err error
	q.iMu.Lock()
	defer q.iMu.Unlock()

	_, err = q.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	buf := indexBufPool.Get().(*[12]byte)

	// uint32 size is 4
	// | -- page(4) -- | -- globalIndex(4) -- |-- localIndex(4) -- |
	binary.LittleEndian.PutUint32((*buf)[0:4], uint32(page))
	binary.LittleEndian.PutUint32((*buf)[4:8], uint32(globalIndex))
	binary.LittleEndian.PutUint32((*buf)[8:12], uint32(localIndex))

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
func (q *Queue[T]) Length() int {
	return len(q.queue)
}

func (q *Queue[T]) initialize(tailGlobalIndex int, tailLocalIndex int) {
	q.qMu.Lock()
	defer q.qMu.Unlock()
	var queueFile *os.File
	isLast := true

	for {
		queueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))

		stat, err := os.Stat(queueFilepath)
		if err != nil {
			if os.IsNotExist(err) {
				queueFile, err = os.Create(queueFilepath)
				if err != nil {
					panic(fmt.Sprintf("could not create file, %s, %v", queueFilepath, err))
				}
				q.queueFile = queueFile
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
		q.queueFile = queueFile
		scanner := bufio.NewScanner(q.queueFile)

		i := 0
		for scanner.Scan() {
			if i < tailGlobalIndex {
				i++
				continue
			}
			b := scanner.Bytes()
			var data []*T
			err = q.decoder(b, &data)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(b), err))
			}
			for j := tailLocalIndex; j < len(data); j++ {
				if isLast {
					isLast = false
					continue
				}
				q.queue <- &Message[T]{
					name:        q.name,
					globalIndex: tailGlobalIndex,
					localIndex:  j,
					data:        data[j],
				}
			}
			tailLocalIndex = 0
			i++
			tailGlobalIndex++
			q.headGlobalIndex = tailGlobalIndex
		}

		if err := scanner.Err(); err != nil {
			panic(fmt.Sprintf("scan error, file: %s, %v", queueFilepath, err))
		}

		nextPage := q.currentPage
		if q.headGlobalIndex == q.queueSize {
			tailGlobalIndex = 0
			q.headGlobalIndex = tailGlobalIndex
			nextPage = q.currentPage + 1
			if nextPage == q.maxPages {
				nextPage = 0
			}
		}

		if nextPage != q.currentPage {
			nextQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, nextPage))
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
			q.currentPage = nextPage
		} else {
			break
		}
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
func (q *Queue[T]) CloseQueue() error {
	q.qMu.Lock()
	defer q.qMu.Unlock()
	close(q.queue)
	err := q.queueFile.Close()
	if err != nil {
		return err
	}
	return nil
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
	q.iMu.Lock()
	defer q.iMu.Unlock()
	err := q.indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}
