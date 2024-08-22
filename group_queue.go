package ffq

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"time"
)

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

type BulkQueueChData[T any] struct {
	data     []*T
	indicies map[string]uint64
}

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

func (gq *GroupQueue[T]) addQueue(name string) error {
	q, err := NewQueue[T](
		name,
		WithFileDir(gq.fileDir),
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

func (gq *GroupQueue[T]) FuncAfterBulkDequeue(batch int, size int, lazy int, f func([]*T) error) error {
	var err error
	dataCh := make(chan BulkQueueChData[T])
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
				dataCh <- BulkQueueChData[T]{
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
								dataCh <- BulkQueueChData[T]{
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

func (gq *GroupQueue[T]) WaitInitialize() {
	<-gq.initializeBlock
}

func (gq *GroupQueue[T]) sendSignal() {
	select {
	case gq.sig <- struct{}{}:
	default:
	}
}
