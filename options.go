// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"errors"
)

type options struct {
	fileDir          *string
	queueSize        *uint64
	enqueueWriteSize *int
	pageSize         *int
	dataFixedLength  *uint64
	jsonEncoder      *func(v any) ([]byte, error)
	jsonDecoder      *func(data []byte, v any) error
}

// Option defines a function type that modifies the options for creating a Queue or GroupQueue.
//
// The function takes a pointer to an options struct and returns an error if the provided option
// is invalid.
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
//	queue, err := NewQueue("myQueue", WithFileDir("/tmp/myQueue"))
func WithFileDir(fileDir string) Option {
	return func(options *options) error {
		options.fileDir = &fileDir
		return nil
	}
}

// WithQueueSize sets the maximum number of items that can be held in the queue.
//
// Parameters:
//   - size: The maximum queue size. Must be greater than 0.
//
// Returns:
//   - Option: An Option function that sets the queueSize in the options struct.
//   - error: Returns an error if the size is less than 1.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithQueueSize(100))
func WithQueueSize(size uint64) Option {
	return func(options *options) error {
		if size < 1 {
			err := errors.New("queueSize must be set to greater than 0")
			return errors.Join(ErrQueueOption, err)
		}
		options.queueSize = &size
		return nil
	}
}

// WithEnqueueWriteSize sets the number of items to write to disk in each batch.
//
// Parameters:
//   - size: The number of items to write. Must be greater than 0.
//
// Returns:
//   - Option: An Option function that sets the enqueueWriteSize in the options struct.
//   - error: Returns an error if the size is less than 1.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithEnqueueWriteSize(10))
func WithEnqueueWriteSize(size int) Option {
	return func(options *options) error {
		if size < 1 {
			err := errors.New("enqueueWriteSize must be set to greater than 0")
			return errors.Join(ErrQueueOption, err)
		}
		options.enqueueWriteSize = &size
		return nil
	}
}

// WithPageSize sets the number of files used in a single rotation cycle.
//
// Parameters:
//   - size: The number of pages. Must be greater than 1.
//
// Returns:
//   - Option: An Option function that sets the pageSize in the options struct.
//   - error: Returns an error if the size is less than 2.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithPageSize(2))
func WithPageSize(size int) Option {
	return func(options *options) error {
		if size < 2 {
			err := errors.New("pageSize must be set to greater than 1")
			return errors.Join(ErrQueueOption, err)
		}
		options.pageSize = &size
		return nil
	}
}

// WithDataFixedLength sets the fixed size of the data block written to each file.
//
// Parameters:
//   - length: The fixed length of the data block. Must be greater than 0.
//
// Returns:
//   - Option: An Option function that sets the dataFixedLength in the options struct.
//   - error: Returns an error if the length is less than 1.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithDataFixedLength(8192))
func WithDataFixedLength(length uint64) Option {
	return func(options *options) error {
		if length < 1 {
			err := errors.New("dataFixedLength must be set to greater than 0")
			return errors.Join(ErrQueueOption, err)
		}
		options.dataFixedLength = &length
		return nil
	}
}

// WithJSONEncoder sets a custom JSON encoder function.
//
// Parameters:
//   - jsonEncoder: A function that encodes data to JSON.
//
// Returns:
//   - Option: An Option function that sets the jsonEncoder in the options struct.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithJSONEncoder(sonic.Marshal))
func WithJSONEncoder(jsonEncoder func(v any) ([]byte, error)) Option {
	return func(options *options) error {
		options.jsonEncoder = &jsonEncoder
		return nil
	}
}

// WithJSONDecoder sets a custom JSON decoder function.
//
// Parameters:
//   - jsonDecoder: A function that decodes data from JSON.
//
// Returns:
//   - Option: An Option function that sets the jsonDecoder in the options struct.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithJSONDecoder(sonic.Unmarshal))
func WithJSONDecoder(jsonDecoder func(data []byte, v any) error) Option {
	return func(options *options) error {
		options.jsonDecoder = &jsonDecoder
		return nil
	}
}
