package ffq

import (
	"errors"
	"testing"
)

func TestWithFileDir(t *testing.T) {
	dirName := "/tmp"
	var options options
	f := WithFileDir(dirName)
	err := f(&options)

	if err != nil {
		t.Errorf("failed test: got is nil, %v", err)
	}

	if *options.fileDir != dirName {
		t.Errorf("failed test: got is equal dirName, %s, %s", *options.fileDir, dirName)
	}
}

func TestWithQueueSize(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
		err   error
	}{
		{
			name:  "queue size can set",
			input: 16,
			err:   nil,
		},
		{
			name:  "queue size cannot set with less than 1",
			input: 0,
			err:   ErrQueueOption,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithQueueSize(tt.input)
			err := f(&options)
			if !errors.Is(err, tt.err) {
				t.Errorf("failed test: got is not equal err, %v, %v", err, tt.err)
			}
			if err == nil && *options.size != tt.input {
				t.Errorf("failed test: got is nil, %v", err)
			}
		})
	}
}

func TestWithMaxPages(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
		err   error
	}{
		{
			name:  "page size can set",
			input: 4,
			err:   nil,
		},
		{
			name:  "page size cannot set with less than 2",
			input: 1,
			err:   ErrQueueOption,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithMaxPage(tt.input)
			err := f(&options)
			if !errors.Is(err, tt.err) {
				t.Errorf("failed test: got is not equal err, %v, %v", err, tt.err)
			}
			if err == nil && *options.maxPage != tt.input {
				t.Errorf("failed test: got is nil, %v", err)
			}
		})
	}
}

func TestWithEncoder(t *testing.T) {
	var encoder func(v any) ([]byte, error) = func(v any) ([]byte, error) { return []byte{0x00}, nil }

	var options options
	f := WithEncoder(encoder)
	err := f(&options)
	if err != nil {
		t.Errorf("failed test: got is nil, %v", err)
	}

	if options.encoder == nil {
		t.Errorf("failed test: got is not nil")
	}
}

func TestWithDecoder(t *testing.T) {
	var decoder func(data []byte, v any) error = func(data []byte, v any) error { return nil }

	var options options
	f := WithDecoder(decoder)
	err := f(&options)
	if err != nil {
		t.Errorf("failed test: got is nil, %v", err)
	}

	if options.decoder == nil {
		t.Errorf("failed test: got is not nil")
	}
}

func TestWithGroupSize(t *testing.T) {
	groupSize := 1
	var options options
	f := WithGroupSize(groupSize)
	err := f(&options)

	if err != nil {
		t.Errorf("failed test: got is nil, %v", err)
	}

	if *options.groupSize != groupSize {
		t.Errorf("failed test: got is equal groupSize, %d, %d", *options.groupSize, groupSize)
	}
}

func TestWithQueueType(t *testing.T) {
	tests := []struct {
		name  string
		input QueueType
	}{
		{
			name:  "SPSC",
			input: SPSC,
		},
		{
			name:  "MPSC",
			input: MPSC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithQueueType(tt.input)
			err := f(&options)
			if err != nil {
				t.Errorf("failed test: got is nil, %v", err)
			}
			if *options.queueType != tt.input {
				t.Errorf("failed test: got is equal input, %v, %v", *options.queueType, tt.input)
			}
		})
	}
}
