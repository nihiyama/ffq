// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
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
	indexFilename  = "index"
	queueFilename  = "queue"
	layoutFilename = "layout"
	indexBufPool   = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
	layoutBufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
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
//   - pageSize: The number of files used in a single rotation cycle.
//   - dataFixedLength: The fixed size of the data block written to each file.
//   - maxFileSize: The maximum size of a single queue file.
//   - maxIndexSize: The maximum size of the index file.
//   - queue: The in-memory queue channel that holds the messages.
//   - headGlobalIndex: The current index of the head of the queue.
//   - currentPage: The current page (file) being written to.
//   - queueFile: The file currently being written to.
//   - indexFile: The file storing the index of the queue.
//   - jsonEncoder: A function to encode data to JSON.
//   - jsonDecoder: A function to decode data from JSON.
//   - initializeBlock: A channel to block until initialization is complete.
type Queue[T any] struct {
	dataFixedLength  uint64
	maxFileSize      uint64
	headSeekStart    uint64
	headSeekEnd      uint64
	queueSize        int
	enqueueWriteSize int
	pageSize         int
	maxIndexSize     int
	headGlobalIndex  uint32
	currentPage      int
	name             string
	fileDir          string
	queue            chan *Message[T]
	queueFile        *os.File
	layoutFile       *os.File
	indexFile        *os.File
	jsonEncoder      func(v any) ([]byte, error)
	jsonDecoder      func(data []byte, v any) error
	initializeBlock  chan struct{}
	qMu              *sync.Mutex
	iMu              *sync.Mutex
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

	enqueueWriteSize := 15
	if options.enqueueWriteSize != nil {
		enqueueWriteSize = *options.enqueueWriteSize
	}

	pageSize := 2
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

	maxFileSize := uint64(queueSize) * dataFixedLength
	maxIndexSize := queueSize * pageSize * enqueueWriteSize

	queue := make(chan *Message[T], queueSize)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	tailGlobalIndex, tailLocalIndex, err := readIndex(indexFilePath)
	if err != nil {
		return nil, err
	}
	headGlobalIndex := tailGlobalIndex + 1
	headLocalIndex := tailLocalIndex + 1
	currentPage := headGlobalIndex / queueSize

	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	initializeBlock := make(chan struct{})
	var qMu sync.Mutex
	var iMu sync.Mutex

	q := Queue[T]{
		name:             name,
		fileDir:          fileDir,
		queueSize:        queueSize,
		enqueueWriteSize: enqueueWriteSize,
		pageSize:         pageSize,
		dataFixedLength:  dataFixedLength,
		maxFileSize:      maxFileSize,
		maxIndexSize:     maxIndexSize,
		currentPage:      currentPage,
		queue:            queue,
		headGlobalIndex:  uint32(headGlobalIndex),
		indexFile:        indexFile,
		jsonEncoder:      jsonEncoder,
		jsonDecoder:      jsonDecoder,
		initializeBlock:  initializeBlock,
		qMu:              &qMu,
		iMu:              &iMu,
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

	jsonData, err := q.jsonEncoder([]*T{data})
	if err != nil {
		return err
	}

	lenBuf := uint64(len(jsonData))
	q.headSeekStart = q.headSeekEnd
	q.headSeekEnd = q.headSeekStart + lenBuf

	// write queue channel
	q.queue <- &Message[T]{
		name:        q.name,
		globalIndex: q.headGlobalIndex,
		localIndex:  0,
		data:        data,
	}

	q.headGlobalIndex++

	err = q.writeLayout(q.headSeekStart, uint64(len(jsonData)))
	if err != nil {
		return err
	}
	err = q.writeQueue(jsonData)
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

	jsonData, err := q.jsonEncoder(data)
	if err != nil {
		return err
	}

	lenData := len(data)
	lenBuf := uint64(len(jsonData))
	q.headSeekStart = q.headSeekEnd
	q.headSeekEnd = q.headSeekStart + lenBuf

	var i uint32
	for i = 0; i < uint32(lenData); i++ {
		q.queue <- &Message[T]{
			name:        q.name,
			globalIndex: q.headGlobalIndex,
			localIndex:  i,
			data:        data[i],
		}
	}

	q.headGlobalIndex++

	err = q.writeLayout(q.headSeekStart, lenBuf)
	if err != nil {
		return err
	}
	err = q.writeQueue(jsonData)
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
	q.iMu.Lock()
	defer q.iMu.Unlock()

	message, ok := <-q.queue
	if !ok {
		return ErrQueueClose
	}

	err = f(message.data)
	if err != nil {
		return err
	}

	err = q.writeIndex(message.globalIndex, message.localIndex)
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
	q.iMu.Lock()
	defer q.iMu.Unlock()

	data := make([]*T, 0, size)
	m, ok := <-q.queue
	if !ok {
		return ErrQueueClose
	}
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
	wiErr := q.writeIndex(globalIndex, localIndex)
	if err != nil {
		err = errors.Join(err, wiErr)
	}
	return err
}

func (q *Queue[T]) writeQueue(b []byte) error {
	var err error
	_, err = q.queueFile.Write(b)
	if err != nil {
		return err
	}

	if int(q.headGlobalIndex) >= q.queueSize*(q.currentPage+1) {
		err = q.rotateFile()
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue[T]) writeLayout(seekStart uint64, bufSize uint64) error {
	var err error

	buf := layoutBufPool.Get().(*bytes.Buffer)
	defer layoutBufPool.Put(buf)

	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, seekStart)
	if err != nil {
		fmt.Println("seekStart write error:", err)
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, bufSize)
	if err != nil {
		fmt.Println("bufSize write error:", err)
		return err
	}

	// uint64 size is 8
	// | -- seekStart(8) -- | -- bufSize(8) -- |
	_, err = q.layoutFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue[T]) rotateFile() error {
	q.queueFile.Close()
	q.layoutFile.Close()
	q.currentPage += 1
	if q.currentPage == q.pageSize {
		q.currentPage = 0
		q.headGlobalIndex = 0
	}
	newQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))
	newLayoutFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", layoutFilename, q.currentPage))
	newQueueFile, err := os.Create(newQueueFilepath)
	if err != nil {
		return err
	}
	newLayoutFile, err := os.Create(newLayoutFilepath)
	if err != nil {
		return err
	}
	q.queueFile = newQueueFile
	q.layoutFile = newLayoutFile
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
	q.iMu.Lock()
	defer q.iMu.Unlock()
	return q.writeIndex(message.globalIndex, message.localIndex)
}

func (q *Queue[T]) writeIndex(globalIndex uint32, localIndex uint32) error {
	var err error

	_, err = q.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	buf := indexBufPool.Get().(*bytes.Buffer)
	defer indexBufPool.Put(buf)

	buf.Reset()
	err = binary.Write(buf, binary.LittleEndian, globalIndex)
	if err != nil {
		fmt.Println("globalIndex write error:", err)
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, localIndex)
	if err != nil {
		fmt.Println("localIndex write error:", err)
		return err
	}

	// int size is 4
	// | -- globalIndex(4) -- | -- localIndex(4) -- |
	_, err = q.indexFile.Write(buf.Bytes())
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
	var (
		queueFile  *os.File
		layoutFile *os.File
	)

	for {
		queueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))
		layoutFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", layoutFilename, q.currentPage))

		stat, err := os.Stat(queueFilepath)
		if err != nil {
			if os.IsNotExist(err) {
				queueFile, err = os.Create(queueFilepath)
				if err != nil {
					panic(fmt.Sprintf("could not create file, %s, %v", queueFilepath, err))
				}
				layoutFile, err = os.Create(layoutFilepath)
				if err != nil {
					panic(fmt.Sprintf("could not create file, %s, %v", layoutFilepath, err))
				}
				q.queueFile = queueFile
				q.layoutFile = layoutFile
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

		// seekPositions slice includes seekStart and bufSize.
		seekPositions := make([]uint64, 0, q.queueSize)
		bufSizes := make([]uint64, 0, q.queueSize)
		layoutFile, err = os.OpenFile(layoutFilepath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("could not open file, %s, %v", layoutFilepath, err))
		}
		q.layoutFile = layoutFile
		for {
			var seekPosition uint64
			var bufSize uint64
			err = binary.Read(q.layoutFile, binary.LittleEndian, &seekPosition)
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				panic(fmt.Sprintf("could not read layoutfile, %v", err))
			}
			seekPositions = append(seekPositions, seekPosition)
			err = binary.Read(q.layoutFile, binary.LittleEndian, &bufSize)
			if err != nil {
				if err.Error() == "EOF" {
					panic(fmt.Sprintf("layoutfile is miss match, %v", err))
				}
				panic(fmt.Sprintf("could not read layoutfile, %v", err))
			}
			bufSizes = append(bufSizes, bufSize)
		}
		start := int(q.headGlobalIndex) - (q.currentPage * q.queueSize)
		for i := start; i < len(seekPositions); i++ {
			q.headSeekStart = seekPositions[i]
			buf := make([]byte, bufSizes[i+1])

			_, err = q.queueFile.Seek(int64(q.headSeekStart), io.SeekStart)
			if err != nil {
				panic(fmt.Sprintf("could not seek file, %s, %v", queueFilepath, err))
			}

			_, err := q.queueFile.Read(buf)
			if err != nil {
				if err == io.EOF {
					if i+1 < len(seekPositions) {
						panic("layout file and queue file miss match")
					}
				}
				panic(fmt.Sprintf("could not read file, %s, %v", queueFilepath, err))
			}
			var data []*T
			err = q.jsonDecoder(buf, &data)
			if err != nil {
				panic(fmt.Sprintf("could not UnMarshal data, %s, %v", string(buf), err))
			}

			for j := localIndex; j < len(data); j++ {
				q.queue <- &Message[T]{
					name:        q.name,
					globalIndex: q.headGlobalIndex,
					localIndex:  uint32(j),
					data:        data[j],
				}
			}
			localIndex = 0
			q.headGlobalIndex++
		}

		// check next page
		if len(seekPositions) == q.queueSize*2 {
			q.currentPage++
			if q.currentPage == q.pageSize {
				q.currentPage = 0
			}
			nextQueueFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, q.currentPage))
			nextLayoutFilepath := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", layoutFilename, q.currentPage))
			nextStat, err := os.Stat(nextQueueFilepath)
			if err != nil {
				if !os.IsNotExist(err) {
					panic(err)
				}
			}
			if stat.ModTime().After(nextStat.ModTime()) {
				os.Remove(nextQueueFilepath)
				os.Remove(nextLayoutFilepath)
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
