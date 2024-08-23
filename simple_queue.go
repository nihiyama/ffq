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

	var fileDir = "/tmp"
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

	var dataFixedLength uint64 = 4 * 1024
	if options.dataFixedLength != nil {
		dataFixedLength = *options.dataFixedLength * 1024
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
	maxIndexSize := queueSize * uint64(pageSize)

	queue := make(chan *Message[T], queueSize)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	tailIndex, err := readIndex(indexFilePath)
	if err != nil {
		return nil, err
	}
	indexFile, err := openIndexFile(indexFilePath)
	if err != nil {
		return nil, err
	}
	tailPosition := tailIndex * dataFixedLength

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
		headIndex:        tailIndex,
		indexFile:        indexFile,
		jsonEncoder:      jsonEncoder,
		jsonDecoder:      jsonDecoder,
	}

	go func() {
		q.initialize(tailPosition)
	}()

	return &q, nil
}

func (q *Queue[T]) Enqueue(data *T) error {
	// write queue file
	var err error
	jsonData, err := q.jsonEncoder([]*T{data})
	if err != nil {
		return err
	}
	paddedData := make([]byte, q.dataFixedLength)
	copy(paddedData, jsonData)

	// write queue channel
	q.queue <- &Message[T]{index: q.headIndex, data: data}
	q.headIndex++
	if q.headIndex == q.maxIndexSize {
		q.headIndex = 0
	}

	err = q.writeFile(paddedData)
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue[T]) BulkEnqueue(data []*T) error {
	var err error
	// if data length is 123, enqueueWriteSize is 10 and dataFixedLength is 4kb
	// paddedData is 12 * 4kb + 4kb
	paddedData := make([]byte, uint64(len(data)/q.enqueueWriteSize)*q.dataFixedLength+q.dataFixedLength)
	l := len(data)
	for i := 0; i < l; i += q.enqueueWriteSize {
		var jsonData []byte
		if l < i+q.enqueueWriteSize {
			jsonData, err = q.jsonEncoder(data[i : i+q.enqueueWriteSize])
		} else {
			jsonData, err = q.jsonEncoder(data[i:l])
		}
		if err != nil {
			return err
		}
		copy(paddedData[uint64(i)*q.dataFixedLength:], jsonData)
		for j := i; j < i; j++ {
			q.queue <- &Message[T]{index: q.headIndex, data: data[j]}
			q.headIndex++
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

func (q *Queue[T]) Dequeue() (*Message[T], error) {
	m, ok := <-q.queue
	if !ok {
		return nil, ErrQueueClose
	}
	return m, nil
}

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

func (q *Queue[T]) UpdateIndex(message Message[T]) error {
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

func (q *Queue[T]) Length() int {
	return len(q.queue)
}

func (q *Queue[T]) initialize(tailPosition uint64) {
	var (
		err       error
		queueFile *os.File
	)
	buffer := make([]byte, q.dataFixedLength)
	for {
		currentPage := int(tailPosition / q.maxFileSize)
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
			q.headIndex = uint64(currentPage) * q.queueSize
			return
		}

		// read queue file and set queue
		queueFile, err = os.OpenFile(queueFilepath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("could not open file, %s, %v", queueFilepath, err))
		}
		pageTailPosition := tailPosition % q.maxFileSize
		_, err = queueFile.Seek(int64(pageTailPosition), io.SeekStart)
		if err != nil {
			panic(fmt.Sprintf("could not seek file, %s, %v", queueFilepath, err))
		}
		for {
			_, err := queueFile.Read(buffer)
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
			for _, d := range data {
				q.queue <- &Message[T]{index: q.headIndex, data: d}
				q.headIndex++
				tailPosition += q.dataFixedLength
			}
		}

		// check next file
		nextPage := int(tailPosition / q.maxFileSize)
		if currentPage == nextPage {
			break
		}
	}

	// release blocking
	q.initializeBlock <- struct{}{}
}

func (q *Queue[T]) WaitInitialize() {
	<-q.initializeBlock
}

func (q *Queue[T]) CloseQueue() error {
	var err error
	close(q.queue)
	err = q.queueFile.Close()
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue[T]) CloseIndex() error {
	err := q.indexFile.Close()
	if err != nil {
		return err
	}
	return nil
}

func createQueueDir(dirName string) error {
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.MkdirAll(dirName, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func openIndexFile(indexFilepath string) (*os.File, error) {
	indexFile, err := os.OpenFile(indexFilepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return indexFile, err
}

func readIndex(indexFilepath string) (uint64, error) {
	var err error
	if _, err := os.Stat(indexFilepath); os.IsNotExist(err) {
		return 0, nil
	}
	indexFile, err := os.Open(indexFilepath)
	if err != nil {
		return 0, err
	}
	// uint64 size is 19
	buffer := make([]byte, 19)
	_, err = indexFile.Read(buffer)
	if err != nil {
		return 0, err
	}
	var index uint64
	_, err = fmt.Sscanf(string(buffer), "%d", &index)
	if err != nil {
		return 0, err
	}
	return index, nil
}
