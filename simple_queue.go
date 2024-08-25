// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var (
	indexFilename = "index"
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
//   - pageSize: The number of files used in a single rotation cycle.
//   - dataFixedLength: The fixed size of the data block written to each file.
//   - maxFileSize: The maximum size of a single queue file.
//   - maxIndexSize: The maximum size of the index file.
//   - queue: The in-memory queue channel that holds the messages.
//   - headIndex: The current index of the head of the queue.
//   - currentPage: The current page (file) being written to.
//   - queueFile: The file currently being written to.
//   - indexFile: The file storing the index of the queue.
//   - jsonEncoder: A function to encode data to JSON.
//   - jsonDecoder: A function to decode data from JSON.
//   - initializeBlock: A channel to block until initialization is complete.
type Queue[T any] struct {
	name             string
	fileDir          string
	queueSize        uint64
	enqueueWriteSize int
	pageSize         int
	dataFixedLength  uint64
	maxFileSize      uint64
	maxIndexSize     uint64
	queue            chan *Message[T]
	headIndex        uint64
	currentPage      int
	queueFile        *os.File
	indexFile        *os.File
	jsonEncoder      func(v any) ([]byte, error)
	jsonDecoder      func(data []byte, v any) error
	initializeBlock  chan struct{}
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

	var queueSize uint64 = 100
	if options.queueSize != nil {
		queueSize = *options.queueSize
	}

	var enqueueWriteSize int = 15
	if options.enqueueWriteSize != nil {
		enqueueWriteSize = *options.enqueueWriteSize
	}

	var pageSize int = 2
	if options.pageSize != nil {
		pageSize = *options.pageSize
	}

	var dataFixedLength uint64 = 4 * 1024 * uint64(enqueueWriteSize)
	if options.dataFixedLength != nil {
		dataFixedLength = *options.dataFixedLength * 1024 * uint64(enqueueWriteSize)
	}

	var jsonEncoder func(v any) ([]byte, error) = json.Marshal
	if options.jsonEncoder != nil {
		jsonEncoder = *options.jsonEncoder
	}

	var jsonDecoder func(data []byte, v any) error = json.Unmarshal
	if options.jsonDecoder != nil {
		jsonDecoder = *options.jsonDecoder
	}

	maxFileSize := queueSize * dataFixedLength
	maxIndexSize := queueSize * uint64(pageSize) * uint64(enqueueWriteSize)

	queue := make(chan *Message[T], queueSize)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	tailIndex, err := readIndex(indexFilePath)
	if err != nil {
		return nil, err
	}
	headIndex := tailIndex + 1

	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	startPosition := (headIndex / uint64(enqueueWriteSize)) * dataFixedLength
	initializeBlock := make(chan struct{})

	q := Queue[T]{
		name:             name,
		fileDir:          fileDir,
		queueSize:        queueSize,
		enqueueWriteSize: enqueueWriteSize,
		pageSize:         pageSize,
		dataFixedLength:  dataFixedLength,
		maxFileSize:      maxFileSize,
		maxIndexSize:     maxIndexSize,
		queue:            queue,
		headIndex:        headIndex,
		indexFile:        indexFile,
		jsonEncoder:      jsonEncoder,
		jsonDecoder:      jsonDecoder,
		initializeBlock:  initializeBlock,
	}

	go func() {
		q.initialize(startPosition)
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
	// write queue file
	var err error
	dataWithNil := make([]*T, q.enqueueWriteSize)
	dataWithNil[0] = data
	jsonData, err := q.jsonEncoder(dataWithNil)
	if err != nil {
		return err
	}
	paddedData := make([]byte, q.dataFixedLength)
	copy(paddedData, jsonData)

	// write queue channel
	q.queue <- &Message[T]{index: q.headIndex, data: data}
	q.headIndex += uint64(q.enqueueWriteSize)
	if q.headIndex == q.maxIndexSize {
		q.headIndex = 0
	}

	err = q.writeFile(paddedData)
	if err != nil {
		return err
	}
	return nil
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
	var err error
	// if data length is 123, enqueueWriteSize is 10 and dataFixedLength is 4kb
	// paddedData is 12 * 4kb + 4kb
	paddedData := make([]byte, uint64(len(data)/q.enqueueWriteSize)*q.dataFixedLength+q.dataFixedLength)
	l := len(data)
	for i := 0; i < l; i += q.enqueueWriteSize {
		var jsonData []byte
		if i+q.enqueueWriteSize < l {
			jsonData, err = q.jsonEncoder(data[i : i+q.enqueueWriteSize])
		} else {
			// create include null data
			dataWithNil := make([]*T, q.enqueueWriteSize)
			copy(dataWithNil, data[i:l])
			jsonData, err = q.jsonEncoder(dataWithNil)
		}
		if err != nil {
			return err
		}
		copy(paddedData[uint64(i/q.enqueueWriteSize)*q.dataFixedLength:], jsonData)
		for j := i; j < i+q.enqueueWriteSize; j++ {
			if j < l {
				q.queue <- &Message[T]{index: q.headIndex, data: data[j]}
				q.headIndex++
			} else {
				q.headIndex += uint64(i + q.enqueueWriteSize - j)
				break
			}
		}
		if q.headIndex == q.maxIndexSize {
			q.headIndex = 0
		}
	}
	err = q.writeFile(paddedData)
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
func (q *Queue[T]) BulkDequeue(size int, lazy int) ([]*Message[T], error) {
	messages := make([]*Message[T], 0, size)
	m, ok := <-q.queue
	if !ok {
		return messages, ErrQueueClose
	}
	messages = append(messages, m)
	timer := time.After(time.Duration(lazy) * time.Millisecond)
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

	err = q.writeIndex(message.index)
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
func (q *Queue[T]) FuncAfterBulkDequeue(size int, lazy int, f func([]*T) error) error {
	var err error

	data := make([]*T, 0, size)
	m, ok := <-q.queue
	if !ok {
		return ErrQueueClose
	}
	index := m.index
	data = append(data, m.data)
	timer := time.After(time.Duration(lazy) * time.Millisecond)
LOOP:
	for {
		select {
		case <-timer:
			break LOOP
		case m, ok := <-q.queue:
			data = append(data, m.data)
			if !ok {
				err = errors.Join(err, ErrQueueClose)
				break LOOP
			}
			index = m.index
			if len(data) == size {
				break LOOP
			}
		}
	}
	fErr := f(data)
	if fErr != nil {
		err = errors.Join(err, fErr)
	}
	wiErr := q.writeIndex(index)
	if err != nil {
		err = errors.Join(err, wiErr)
	}
	return err
}

func (q *Queue[T]) writeFile(data []byte) error {
	offset := 0
	stat, err := q.queueFile.Stat()
	if err != nil {
		return err
	}
	fileSize := uint64(stat.Size())
	space := q.maxFileSize - fileSize
	for offset < len(data) {
		writeSize := uint64(len(data) - offset)
		if writeSize > space {
			writeSize = space
		}

		_, err = q.queueFile.Write(data[offset : offset+int(writeSize)])
		if err != nil {
			return err
		}
		space -= writeSize
		offset += int(writeSize)

		if space == 0 {
			err = q.rotateFile()
			if err != nil {
				return err
			}
			stat, err = q.queueFile.Stat()
			if err != nil {
				return err
			}
			fileSize = uint64(stat.Size())
			space = q.maxFileSize - fileSize
		}
	}
	return nil
}

func (q *Queue[T]) rotateFile() error {
	q.queueFile.Close()
	q.currentPage += 1
	if q.currentPage == q.pageSize {
		q.currentPage = 0
	}
	newFile, err := os.Create(filepath.Join(q.fileDir, fmt.Sprintf("queue.%d", q.currentPage)))
	if err != nil {
		return err
	}
	q.queueFile = newFile
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
	return q.writeIndex(message.index)
}

func (q *Queue[T]) writeIndex(index uint64) error {
	var err error

	_, err = q.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// uin64 size is 19
	indexStr := fmt.Sprintf("%0*d", 19, index)
	_, err = q.indexFile.Write([]byte(indexStr))
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

func (q *Queue[T]) initialize(startPosition uint64) {
	var (
		err       error
		queueFile *os.File
	)
	buffer := make([]byte, q.dataFixedLength)
	startIndex := startPosition / q.dataFixedLength * uint64(q.enqueueWriteSize)
	isStart := true
	for {
		currentPage := int(startPosition / q.maxFileSize)
		if currentPage == q.pageSize {
			currentPage = 0
		}
		queueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("queue.%d", currentPage))

		// queue file is not exist
		if _, err := os.Stat(queueFilepath); os.IsNotExist(err) {
			// create queue file and return
			queueFile, err = os.Create(queueFilepath)
			if err != nil {
				panic(fmt.Sprintf("could not create file, %s, %v", queueFilepath, err))
			}
			q.queueFile = queueFile
			q.currentPage = currentPage
			q.headIndex = uint64(currentPage) * q.queueSize * uint64(q.enqueueWriteSize)
			break
		}

		// read queue file and set queue
		queueFile, err = os.OpenFile(queueFilepath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("could not open file, %s, %v", queueFilepath, err))
		}
		q.queueFile = queueFile
		pageStartPosition := startPosition % q.maxFileSize
		_, err = q.queueFile.Seek(int64(pageStartPosition), io.SeekStart)
		if err != nil {
			panic(fmt.Sprintf("could not seek file, %s, %v", queueFilepath, err))
		}
		for {
			_, err := q.queueFile.Read(buffer)
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(fmt.Sprintf("could not read file, %s, %v", queueFilepath, err))
			}
			var data []*T
			err = q.jsonDecoder(bytes.Trim(buffer, "\x00"), &data)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(buffer), err))
			}
			if isStart {
				for _, d := range data[q.headIndex-startIndex:] {
					if d != nil {
						q.queue <- &Message[T]{index: q.headIndex, data: d}
					}
					q.headIndex++
				}
				isStart = false
			} else {
				for _, d := range data {
					if d != nil {
						q.queue <- &Message[T]{index: q.headIndex, data: d}
					}
					q.headIndex++
				}
				startPosition += q.dataFixedLength
			}
		}

		// check next file
		nextPage := int(startPosition / q.maxFileSize)
		if currentPage == nextPage {
			break
		}
	}

	// release blocking
	q.initializeBlock <- struct{}{}
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
