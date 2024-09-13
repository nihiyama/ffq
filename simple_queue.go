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
	indexBufPool  = sync.Pool{
		New: func() interface{} {
			var b [12]byte
			return &b
		},
	}
	queueBufPool = sync.Pool{
		New: func() interface{} {
			// default 64kb Pool
			return bytes.NewBuffer(make([]byte, 0, 64*1024))
		},
	}
)

// Queue represents a file-based FIFO queue with generic type T.
// It manages the queue data in files and provides methods for enqueueing, dequeueing, and
// handling data in a type-safe manner.
//
// Fields:
//   - name: The name of the queue.
//   - fileDir: The directory where the queue files are stored.
//   - queueSize: The maximum number of items that can be held in the queue.
//   - enqueueWriteSize: The number of items to write to disk in each batch.
//   - maxPages: The number of files used in a single rotation cycle.
//   - dataFixedLength: The fixed size of the data block written to each file.
//   - maxFileSize: The maximum size of a single queue file.
//   - maxIndexSize: The maximum size of the index file.
//   - queue: The in-memory queue channel that holds the messages.
//   - headGlobalIndex: The current index of the head of the queue.
//   - currentPage: The current page (file) being written to.
//   - queueFile: The file currently being written to.
//   - indexFile: The file storing the index of the queue.
//   - encoder: A function to encode data to JSON.
//   - decoder: A function to decode data from JSON.
//   - initializeBlock: A channel to block until initialization is complete.
type Queue[T any] struct {
	queueSize       int
	maxPages        int
	currentPage     int
	headGlobalIndex int
	name            string
	fileDir         string
	queue           chan *Message[T]
	queueFile       *os.File
	indexFile       *os.File
	encoder         func(v any) ([]byte, error)
	decoder         func(data []byte, v any) error
	initializeBlock chan struct{}
	qMu             *sync.Mutex
	iMu             *sync.Mutex
}

// NewQueue creates a new file-based FIFO queue.
//
// Parameters:
//   - name: The name of the queue.
//   - opts: A variadic list of options to customize the queue settings.
//
// Returns:
//   - *Queue[T]: A pointer to the newly created queue instance.
//   - error: An error if the queue could not be created.
//
// Example:
//
//	queue, err := NewQueue[Data]("myQueue")
//	if err != nil {
//	  log.Fatal(err)
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
	headGlobalIndex := tailGlobalIndex + 1
	headLocalIndex := tailLocalIndex + 1

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
		headGlobalIndex: headGlobalIndex,
		indexFile:       indexFile,
		encoder:         encoder,
		decoder:         decoder,
		initializeBlock: initializeBlock,
		qMu:             &qMu,
		iMu:             &iMu,
	}

	go func() {
		q.initialize(headLocalIndex)
	}()

	return &q, nil
}

// Enqueue adds a single item to the queue.
//
// Parameters:
//   - data: A pointer to the item to be added to the queue.
//
// Returns:
//   - error: An error if the item could not be added to the queue.
//
// Example:
//
//	data := Data{...}
//	err := queue.Enqueue(&data)
//	if err != nil {
//	  log.Fatal(err)
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
//   - data: A slice of pointers to the items to be added to the queue.
//
// Returns:
//   - error: An error if the items could not be added to the queue.
//
// Example:
//
//	data := []*Data{{...},{...},...}
//	err := queue.BulkEnqueue(data)
//	if err != nil {
//		log.Fatal(err)
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

// Dequeue removes and returns a single item from the queue.
//
// Returns:
//   - *Message[T]: The dequeued item wrapped in a Message struct.
//   - error: An error if the queue is closed or empty.
//
// Example:
//
//	message, err := queue.Dequeue()
//	if err != nil {
//	  log.Fatal(err)
//	}
func (q *Queue[T]) Dequeue() (*Message[T], error) {
	m, ok := <-q.queue
	if !ok {
		return nil, ErrQueueClose
	}
	return m, nil
}

// BulkDequeue removes and returns multiple items from the queue, waiting up to a specified duration.
//
// Parameters:
//   - size: The maximum number of items to dequeue.
//   - lazy: The time to wait for more items before returning.
//
// Returns:
//   - []*Message[T]: A slice of dequeued items wrapped in Message structs.
//   - error: An error if the queue is closed or empty.
//
// Example:
//
//	messages, err := queue.BulkDequeue(10, 500)
//	if err != nil {
//	  log.Fatal(err)
//	}
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

// FuncAfterDequeue applies a function to the data of a dequeued item and updates the index.
//
// Parameters:
//   - f: A function that takes a pointer to the dequeued data and returns an error.
//
// Returns:
//   - error: An error if the function or index update fails.
//
// Example:
//
//	err := queue.FuncAfterDequeue(func(data Data) error {
//	  fmt.Println(*data)
//	  return nil
//	})
//	if err != nil {
//	  log.Fatal(err)
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

// FuncAfterBulkDequeue applies a function to the data of multiple dequeued items and updates the index.
//
// Parameters:
//   - size: The maximum number of items to dequeue.
//   - lazy: The time to wait for more items before applying the function.
//   - f: A function that takes a slice of pointers to the dequeued data and returns an error.
//
// Returns:
//   - error: An error if the function or index update fails.
//
// Example:
//
//	err := queue.FuncAfterBulkDequeue(10, 500, func(data []*Data) error {
//	  for _, d := range data {
//	    fmt.Println(*d)
//	  }
//	  return nil
//	})
//	if err != nil {
//	  log.Fatal(err)
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
	defer queueBufPool.Put(buf)

	buf.Reset()
	buf.Grow(len(b) + 1)

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

	if int(q.headGlobalIndex) >= q.queueSize*(int(q.currentPage)+1) {
		err = q.rotateFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue[T]) rotateFile() error {
	q.queueFile.Close()
	q.headGlobalIndex = 0
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

// UpdateIndex updates the index file with the provided message's index.
//
// Parameters:
//   - message: The message containing the index to be written to the index file.
//
// Returns:
//   - error: An error if the index could not be updated.
//
// Example:
//
//	err := queue.UpdateIndex(message)
//	if err != nil {
//	  log.Fatal(err)
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
	defer indexBufPool.Put(buf)

	// uint32 size is 4
	// | -- page(4) -- | -- globalIndex(4) -- |-- localIndex(4) -- |
	binary.LittleEndian.PutUint32((*buf)[0:4], uint32(page))
	binary.LittleEndian.PutUint32((*buf)[4:8], uint32(globalIndex))
	binary.LittleEndian.PutUint32((*buf)[8:12], uint32(localIndex))

	_, err = q.indexFile.Write((*buf)[:])
	if err != nil {
		return err
	}
	return nil
}

// Lengt1 returns the current length of the queue.
//
// Returns:
//   - int: The number of items currently in the queue.
//
// Example:
//
//	length := queue.Length()
//	fmt.Println("Queue length:", length)
func (q *Queue[T]) Length() int {
	return len(q.queue)
}

func (q *Queue[T]) initialize(localIndex int) {
	var queueFile *os.File

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
			i++
			if i <= q.headGlobalIndex {
				continue
			}
			b := scanner.Bytes()
			var data []*T
			err = q.decoder(b, &data)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(b), err))
			}
			for j := localIndex; j < len(data); j++ {
				q.queue <- &Message[T]{
					name:        q.name,
					globalIndex: q.headGlobalIndex,
					localIndex:  j,
					data:        data[j],
				}
			}
			localIndex = 0
			q.headGlobalIndex++
		}

		if err := scanner.Err(); err != nil {
			panic(fmt.Sprintf("scan error, file: %s, %v", queueFilepath, err))
		}

		nextPage := q.currentPage
		if q.headGlobalIndex == q.queueSize {
			nextPage := q.currentPage + 1
			if nextPage == q.maxPages {
				nextPage = 0
			}
		}

		if nextPage != q.currentPage {
			nextQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))
			nextStat, err := os.Stat(nextQueueFilepath)
			if err != nil {
				if !os.IsNotExist(err) {
					panic(err)
				}
			}
			if stat.ModTime().After(nextStat.ModTime()) {
				os.Remove(nextQueueFilepath)
			}
		} else {
			break
		}
	}

	// release blocking
	q.initializeBlock <- struct{}{}
	close(q.initializeBlock)
}

// WaitInitialize blocks until the queue initialization is complete.
//
// Example:
//
//	queue.WaitInitialize()
//	fmt.Println("Queue initialized")
func (q *Queue[T]) WaitInitialize() {
	<-q.initializeBlock
}

// CloseQueue closes the queue file and releases associated resources.
//
// Returns:
//   - error: An error if the queue file could not be closed.
//
// Example:
//
//	err := queue.CloseQueue()
//	if err != nil {
//	  log.Fatal(err)
//	}
func (q *Queue[T]) CloseQueue() error {
	var err error
	close(q.queue)
	err = q.queueFile.Close()
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
	err := q.indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}
