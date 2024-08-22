package ffq

import (
	"errors"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	ErrQueueOption   = errors.New("queue option input error")
	ErrQueueClose    = errors.New("queue close error")
)

func IsErrQueueNotFound(err error) bool {
	return errors.Is(err, ErrQueueNotFound)
}

func IsErrQueueOption(err error) bool {
	return errors.Is(err, ErrQueueOption)
}

func IsErrQueueClose(err error) bool {
	return errors.Is(err, ErrQueueClose)
}
