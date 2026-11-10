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

	writeOffset    int64
	writeNeedsLF   bool
	rotatePending  bool
	sequence       uint64 // Process-local ordering only; never persisted.
	ackNext        atomic.Uint64
	pageEnds       []uint64
	pagePresent    []bool
	initialIndex   *uint64
	initErr        error
	ready          atomic.Bool
	failure        atomic.Pointer[queueFailure]
	failureSig     chan struct{}
	stop           chan struct{}
	progress       chan struct{}
	queueCloseOnce sync.Once
	group          *GroupQueue[T]
	parentFailure  <-chan struct{}
	mu             sync.Mutex // Mutex for synchronizing multi-producer operations
}

// NewQueue creates a new Queue with the specified name and options.
//
// Parameters:
//   - name: The identifier for the queue. This is used to distinguish multiple queues.
//   - opts: A list of options to customize the queue (e.g., WithQueueSize, WithMaxPage, WithFileDir, WithEncoder, WithDecoder).
//
// Returns:
//   - *Queue[T]: A pointer to the newly created and partially initialized Queue.
//   - error: An error if the queue initialization fails (for example, if creating the file directory fails).
//
// Example:
//
//	// Create a queue with a buffer size of 100 items and up to 5 file pages.
//	q, err := ffq.NewQueue[string]("myQueue", ffq.WithQueueSize(100), ffq.WithMaxPage(5))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
//	// Start the consumer first, then check initialization before starting producers.
//	if err := q.WaitInitializeWithError(); err != nil {
//	    log.Fatal(err)
//	}
func NewQueue[T any](name string, opts ...Option) (*Queue[T], error) {
	return newQueue[T](name, nil, opts...)
}

func newQueue[T any](name string, group *GroupQueue[T], opts ...Option) (*Queue[T], error) {
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

	if size > uint64(int(^uint(0)>>1)) || maxPage > uint64(int(^uint(0)>>1)) || size > ^uint64(0)/maxPage || (queueType != SPSC && queueType != MPSC) || encoder == nil || decoder == nil {
		return nil, fmt.Errorf("%w: invalid queue capacity, type, or codec", ErrQueueOption)
	}
	queue := make(chan *Message[T], size)

	// open index file
	indexFilePath := filepath.Join(fileDir, indexFilename)
	index, err := loadQueueIndex(indexFilePath)
	if err != nil {
		return nil, err
	}
	if index != nil && *index >= size*maxPage {
		return nil, fmt.Errorf("initialize queue: index outside capacity")
	}

	var tail uint64 = 0
	if index != nil {
		tail = (*index + 1) % (size * maxPage)
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
		initialIndex:    index,
		pageEnds:        make([]uint64, maxPage),
		pagePresent:     make([]bool, maxPage),
		failureSig:      make(chan struct{}),
		stop:            make(chan struct{}),
		progress:        make(chan struct{}, 1),
		group:           group,
	}

	if group != nil {
		q.parentFailure = group.failureSig
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
	err = q.writeQueue(buf, tail, 1)
	if err != nil {
		return err
	}

	m := &Message[T]{
		index:    tail,
		sequence: q.sequence,
		owner:    q,
		item:     item,
	}
	q.sequence++
	return q.publish(m)
}

func (q *Queue[T]) bulkEnqueue(tail uint64, items []*T) error {
	buf, err := q.encoder(items)
	if err != nil {
		return err
	}
	err = q.writeQueue(buf, tail, uint64(len(items)))
	if err != nil {
		return err
	}

	for _, item := range items {
		m := &Message[T]{
			index:    tail,
			sequence: q.sequence,
			owner:    q,
			item:     item,
		}
		q.sequence++
		if err := q.publish(m); err != nil {
			return err
		}
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
	if err := q.canEnqueue(); err != nil {
		return err
	}
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
	if err := q.canEnqueue(); err != nil {
		return err
	}
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
	if err := q.currentFailure(); err != nil {
		return nil, err
	}
	select {
	case m, ok := <-q.queue:
		return q.received(m, ok)
	default:
	}
	select {
	case <-q.failureSig:
		return nil, q.failure.Load().err
	case <-q.parentFailure:
		return nil, q.group.failure.Load().err
	case m, ok := <-q.queue:
		return q.received(m, ok)
	}
}

// BulkDequeue retrieves multiple messages from the queue in a single operation.
// It waits for either the specified number of messages or until the lazy duration expires.
//
// Parameters:
//   - size: The maximum number of messages to dequeue at once. Zero returns ErrQueueOption without consuming messages.
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
	if size == 0 {
		return nil, fmt.Errorf("%w: bulk size must be greater than 0", ErrQueueOption)
	}
	var err error
	ms := make([]*Message[T], 0, size)

	m, err := q.Dequeue()
	if err != nil {
		return nil, err
	}
	ms = append(ms, m)
	if size == 1 {
		return ms, nil
	}

	timer := time.After(lazy)
	for {
		select {
		case <-timer:
			return ms, nil
		case <-q.failureSig:
			return ms, q.failure.Load().err
		case <-q.parentFailure:
			return ms, q.group.failure.Load().err
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
	iErr := q.UpdateIndex(m)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return err
}

// FuncAfterBulkDequeue retrieves multiple items from the queue as a batch,
// applies the specified function to the batch, and then updates the index file based on the last item processed.
//
// Parameters:
//   - size: The maximum number of items to dequeue in the batch. Zero returns ErrQueueOption without invoking f.
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
	if size == 0 {
		return 0, fmt.Errorf("%w: bulk size must be greater than 0", ErrQueueOption)
	}
	var err error
	items := make([]*T, 0, size)

	m, err := q.Dequeue()
	if err != nil {
		return 0, err
	}
	lastM := m
	items = append(items, m.item)

	if size > 1 {
		timer := time.After(lazy)
	LOOP:
		for {
			select {
			case <-timer:
				break LOOP
			case <-q.failureSig:
				return 0, q.failure.Load().err
			case <-q.parentFailure:
				return 0, q.group.failure.Load().err
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
	}
	fErr := f(items)
	if fErr != nil {
		err = errors.Join(err, fErr)
	}
	iErr := q.UpdateIndex(lastM)
	if iErr != nil {
		err = errors.Join(err, iErr)
	}
	return len(items), err
}

func (q *Queue[T]) writeQueue(b []byte, tail, count uint64) error {
	if err := q.currentFailure(); err != nil {
		return err
	}
	var err error

	// file rotation
	currentPage := atomic.LoadUint64(&q.currentPage)
	if (tail/q.size) != currentPage || q.rotatePending {
		err = q.rotatePage(tail / q.size)
		if err != nil {
			return err
		}
	}

	buf := queueBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Grow(len(b) + 1)
	defer queueBufPool.Put(buf)

	if q.writeNeedsLF {
		buf.WriteByte('\n')
	}
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
	offset := q.writeOffset
	if q.sequence > ^uint64(0)-count {
		return q.failIO("sequence overflow", fmt.Errorf("sequence overflow"))
	}
	_, err = buf.WriteTo(queueFile)
	if err != nil {
		// Never append a retry after a partially written JSON record.
		rollback := errors.Join(queueFile.Truncate(offset), func() error { _, e := queueFile.Seek(offset, io.SeekStart); return e }())
		if rollback != nil {
			return q.failIO("rollback queue write", errors.Join(err, rollback))
		}
		return err
	}
	q.writeOffset += int64(len(b) + 1)
	if q.writeNeedsLF {
		q.writeOffset++
		q.writeNeedsLF = false
	}
	page := atomic.LoadUint64(&q.currentPage)
	q.pageEnds[page] = q.sequence + count
	q.pagePresent[page] = true
	return nil
}

func (q *Queue[T]) rotateFile() error {
	return q.rotatePage((atomic.LoadUint64(&q.currentPage) + 1) % q.maxPage)
}

func (q *Queue[T]) rotatePage(page uint64) error {
	if err := q.waitPage(page); err != nil {
		return err
	}
	path := filepath.Join(q.fileDir, fmt.Sprintf("%s.%d", queueFilename, page))
	next, err := os.OpenFile(path, fCreateFlag, 0644)
	if err != nil {
		return err
	}
	old := q.queueFile.Load().(*os.File)
	q.queueFile.Store(next)
	atomic.StoreUint64(&q.currentPage, page)
	q.pagePresent[page] = false
	q.pageEnds[page] = q.sequence
	q.writeOffset, q.writeNeedsLF, q.rotatePending = 0, false, false
	if err := old.Close(); err != nil {
		return q.failIO("close old page", err)
	}
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
	if m == nil || m.owner != q {
		return fmt.Errorf("queue progress: message belongs to another instance")
	}
	return q.writeIndex(m.index, m.sequence)
}

func (q *Queue[T]) writeIndex(index, sequence uint64) error {
	if err := q.currentFailure(); err != nil {
		return err
	}
	if q.isIndexClosed.Load() {
		return os.ErrClosed
	}
	previous := q.ackNext.Load()
	if sequence == ^uint64(0) || sequence+1 < previous {
		return fmt.Errorf("queue progress: backward index")
	}
	if sequence+1 == previous {
		return nil
	}
	buf := indexBufPool.Get().(*[8]byte)
	defer indexBufPool.Put(buf)
	binary.LittleEndian.PutUint64(buf[:], index)
	n, err := q.indexFile.WriteAt(buf[:], 0)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return q.failIO("write index", err)
	}
	q.ackNext.Store(sequence + 1)
	select {
	case q.progress <- struct{}{}:
	default:
	}
	return nil
}

// Length returns the current number of messages stored in the internal buffer of the queue.
// Messages held by the consumer are excluded; this is not a completion count.
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
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return 0, fmt.Errorf("trailing JSON data")
	}
	return n, nil
}

// WaitInitialize blocks until the queue has completed its background initialization process.
// Since the queue may load existing data from files and perform recovery in the background,
// start the consumer before calling this method and start producers afterward.
// This method does not report initialization failures. Use WaitInitializeWithError
// when startup must check the initialization result.
//
// Example:
//
//	// Start the consumer before waiting for initialization to complete.
//	q.WaitInitialize()
//	fmt.Println("Queue initialization complete")
func (q *Queue[T]) WaitInitialize() {
	<-q.initializeBlock
}

// WaitInitializeWithError waits for background initialization to finish, then
// returns an initialization error, a recorded queue or parent-group failure,
// or ErrQueueClose if the queue is closed. Start the consumer before waiting.
// A nil result does not guarantee that later operations will succeed or that
// recovered messages have finished processing. Continue checking operation errors.
// This method neither consumes messages nor closes the queue.
//
// Example:
//
//	// Start the consumer first. Start producers only after this succeeds.
//	if err := q.WaitInitializeWithError(); err != nil {
//	    panic(err)
//	}
func (q *Queue[T]) WaitInitializeWithError() error {
	<-q.initializeBlock
	if err := q.currentFailure(); err != nil {
		return err
	}
	if q.initErr != nil {
		return q.initErr
	}
	if q.isQueueClosed.Load() {
		return ErrQueueClose
	}
	return nil
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
	if !q.isQueueClosed.CompareAndSwap(false, true) {
		return nil
	}
	close(q.stop)
	<-q.initializeBlock
	q.queueCloseOnce.Do(func() { close(q.queue) })
	if file := q.queueFile.Load(); file != nil {
		return file.(*os.File).Close()
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
	// Recovery can still publish messages until initialization completes.
	<-q.initializeBlock
	if !q.isIndexClosed.CompareAndSwap(false, true) {
		return nil
	}
	return q.indexFile.Close()
}
