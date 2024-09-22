// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"errors"
)

type options struct {
	fileDir   *string
	queueSize *int
	maxPages  *int
	encoder   *func(v any) ([]byte, error)
	decoder   *func(data []byte, v any) error
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
func WithQueueSize(size int) Option {
	return func(options *options) error {
		if size < 1 {
			err := errors.New("queueSize must be set to greater than 0")
			return errors.Join(ErrQueueOption, err)
		}
		options.queueSize = &size
		return nil
	}
}

// WithMaxPages sets the number of files used in a single rotation cycle.
//
// Parameters:
//   - size: The number of pages. Must be greater than 1.
//
// Returns:
//   - Option: An Option function that sets the maxPages in the options struct.
//   - error: Returns an error if the size is less than 2.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithMaxPages(2))
func WithMaxPages(size int) Option {
	return func(options *options) error {
		if size < 2 {
			err := errors.New("maxPages must be set to greater than 1")
			return errors.Join(ErrQueueOption, err)
		}
		options.maxPages = &size
		return nil
	}
}

// WithEncoder sets a custom JSON encoder function.
//
// Parameters:
//   - encoder: A function that encodes data to JSON.
//
// Returns:
//   - Option: An Option function that sets the encoder in the options struct.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithEncoder(sonic.Marshal))
func WithEncoder(encoder func(v any) ([]byte, error)) Option {
	return func(options *options) error {
		options.encoder = &encoder
		return nil
	}
}

// WithDecoder sets a custom JSON decoder function.
//
// Parameters:
//   - decoder: A function that decodes data from JSON.
//
// Returns:
//   - Option: An Option function that sets the decoder in the options struct.
//
// Example:
//
//	queue, err := NewQueue("myQueue", WithDecoder(sonic.Unmarshal))
func WithDecoder(decoder func(data []byte, v any) error) Option {
	return func(options *options) error {
		options.decoder = &decoder
		return nil
	}
}
