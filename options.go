package ffq

import (
	"errors"
)

// options is
type options struct {
	fileDir          *string
	queueSize        *uint64
	enqueueWriteSize *int
	pageSize         *int
	dataFixedLength  *uint64
	jsonEncoder      *func(v any) ([]byte, error)
	jsonDecoder      *func(data []byte, v any) error
}

type Option func(options *options) error

func WithFileDir(fileDir string) Option {
	return func(options *options) error {
		options.fileDir = &fileDir
		return nil
	}
}

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

func WithJSONEncoder(jsonEncoder func(v any) ([]byte, error)) Option {
	return func(options *options) error {
		options.jsonEncoder = &jsonEncoder
		return nil
	}
}

func WithJSONDecoder(jsonDecoder func(data []byte, v any) error) Option {
	return func(options *options) error {
		options.jsonDecoder = &jsonDecoder
		return nil
	}
}
