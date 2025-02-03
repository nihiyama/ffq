// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"errors"
)

type options struct {
	size      *uint64                         // Maximum number of items per file page.
	maxPage   *uint64                         // Total number of file pages to be used in rotation.
	groupSize *int                            // (For GroupQueue) The group size configuration.
	fileDir   *string                         // Directory where the queue files are stored.
	queueType *QueueType                      // The mode of the queue (e.g., SPSC or MPSC).
	encoder   *func(v any) ([]byte, error)    // Custom function for encoding data.
	decoder   *func(data []byte, v any) error // Custom function for decoding data.
}

// Option defines a function type that modifies the options for creating a Queue or GroupQueue.
// An Option function accepts a pointer to an options struct and returns an error if the provided
// option is invalid.
type Option func(options *options) error

// WithFileDir sets the directory where the queue files are stored.
//
// Parameters:
//   - fileDir: The directory path to be used for storing queue files.
//
// Returns:
//   - Option: An Option function that sets the fileDir in the options struct.
//
// Example:
//
//	q, err := NewQueue("myQueue", WithFileDir("/tmp/myQueue"))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithFileDir(fileDir string) Option {
	return func(options *options) error {
		options.fileDir = &fileDir
		return nil
	}
}

// WithQueueSize sets the maximum number of items that can be held in one file page of the queue.
//
// Parameters:
//   - size: The maximum number of items per file page. The value must be greater than 0.
//
// Returns:
//   - Option: An Option function that sets the queue size in the options struct.
//   - error: An error if the provided size is less than 1.
//
// Example:
//
//	q, err := NewQueue("myQueue", WithQueueSize(100))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithQueueSize(size uint64) Option {
	return func(options *options) error {
		if size < 1 {
			err := errors.New("queueSize must be set to greater than 0")
			return errors.Join(ErrQueueOption, err)
		}
		options.size = &size
		return nil
	}
}

// WithMaxPage sets the number of file pages used in a single rotation cycle.
//
// Parameters:
//   - page: The number of pages. The value must be greater than 1.
//
// Returns:
//   - Option: An Option function that sets the maxPage in the options struct.
//   - error: An error if the provided page value is less than 2.
//
// Example:
//
//	q, err := ffq.NewQueue("myQueue", ffq.WithMaxPage(2))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithMaxPage(page uint64) Option {
	return func(options *options) error {
		if page < 2 {
			err := errors.New("maxPages must be set to greater than 1")
			return errors.Join(ErrQueueOption, err)
		}
		options.maxPage = &page
		return nil
	}
}

// WithEncoder sets a custom encoder function for serializing data before it is written to the queue.
//
// Parameters:
//   - encoder: A function that encodes a value of any type into a byte slice.
//     For example, json.Marshal can be used as a default encoder.
//
// Returns:
//   - Option: An Option function that sets the encoder in the options struct.
//
// Example:
//
//	q, err := ffq.NewQueue("myQueue", ffq.WithEncoder(sonic.Marshal))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithEncoder(encoder func(v any) ([]byte, error)) Option {
	return func(options *options) error {
		options.encoder = &encoder
		return nil
	}
}

// WithDecoder sets a custom decoder function for deserializing data read from the queue.
//
// Parameters:
//   - decoder: A function that decodes a byte slice into a value of any type.
//     For example, json.Unmarshal can be used as a default decoder.
//
// Returns:
//   - Option: An Option function that sets the decoder in the options struct.
//
// Example:
//
//	q, err := ffq.NewQueue("myQueue", ffq.WithDecoder(sonic.Unmarshal))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithDecoder(decoder func(data []byte, v any) error) Option {
	return func(options *options) error {
		options.decoder = &decoder
		return nil
	}
}

// WithQueueType sets the operating mode of the queue (e.g., SPSC or MPSC).
//
// Parameters:
//   - queueType: The mode of the queue. For example, SPSC for Single Producer Single Consumer,
//     or MPSC for Multiple Producer Single Consumer.
//
// Returns:
//   - Option: An Option function that sets the queueType in the options struct.
//
// Example:
//
//	q, err := ffq.NewQueue("myQueue", ffq.WithQueueType(ffq.SPSC))
//	if err != nil {
//	    log.Fatalf("Failed to create queue: %v", err)
//	}
func WithQueueType(queueType QueueType) Option {
	return func(options *options) error {
		options.queueType = &queueType
		return nil
	}
}

// WithGroupSize sets the group size for a GroupQueue.
//
// Parameters:
//   - size: The group size as an integer. The specific use of group size depends on the GroupQueue implementation.
//
// Returns:
//   - Option: An Option function that sets the groupSize in the options struct.
//
// Example:
//
//	gq, err := ffq.NewGroupQueue("myGroupQueue", ffq.WithGroupSize(10))
//	if err != nil {
//	    log.Fatalf("Failed to create group queue: %v", err)
//	}
func WithGroupSize(size int) Option {
	return func(options *options) error {
		options.groupSize = &size
		return nil
	}
}
