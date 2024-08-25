// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"errors"
)

var (
	// ErrQueueNotFound is returned when the specified queue cannot be found.
	ErrQueueNotFound = errors.New("queue not found")

	// ErrQueueOption is returned when there is an error with the queue options provided during initialization.
	ErrQueueOption = errors.New("queue option input error")

	// ErrQueueClose is returned when an operation is attempted on a closed queue.
	ErrQueueClose = errors.New("queue close error")
)

// IsErrQueueNotFound checks if the given error is ErrQueueNotFound.
//
// Parameters:
//   - err: The error to check.
//
// Returns:
//   - bool: True if the error is ErrQueueNotFound, false otherwise.
//
// Example:
//
//	if IsErrQueueNotFound(err) {
//	  fmt.Println("Queue not found")
//	}
func IsErrQueueNotFound(err error) bool {
	return errors.Is(err, ErrQueueNotFound)
}

// IsErrQueueOption checks if the given error is ErrQueueOption.
//
// Parameters:
//   - err: The error to check.
//
// Returns:
//   - bool: True if the error is ErrQueueOption, false otherwise.
//
// Example:
//
//	if IsErrQueueOption(err) {
//	  fmt.Println("Queue option input error")
//	}
func IsErrQueueOption(err error) bool {
	return errors.Is(err, ErrQueueOption)
}

// IsErrQueueClose checks if the given error is ErrQueueClose.
//
// Parameters:
//   - err: The error to check.
//
// Returns:
//   - bool: True if the error is ErrQueueClose, false otherwise.
//
// Example:
//
//	if IsErrQueueClose(err) {
//	  fmt.Println("Queue has been closed")
//	}
func IsErrQueueClose(err error) bool {
	return errors.Is(err, ErrQueueClose)
}
