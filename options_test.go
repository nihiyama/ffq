package ffq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBoundaryQueueOptions(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			opts      []Option
			invalid   bool
			groupOnly bool
		}{
			{name: "minimum queue size", opts: []Option{WithQueueSize(1)}},
			{name: "zero queue size", opts: []Option{WithQueueSize(0)}, invalid: true},
			{name: "minimum pages", opts: []Option{WithMaxPage(2)}},
			{name: "one page", opts: []Option{WithMaxPage(1)}, invalid: true},
			{name: "zero pages", opts: []Option{WithMaxPage(0)}, invalid: true},
			{name: "queue size exceeds int", opts: []Option{WithQueueSize(uint64(^uint(0)>>1) + 1)}, invalid: true},
			{name: "page count exceeds int", opts: []Option{WithMaxPage(uint64(^uint(0)>>1) + 1)}, invalid: true},
			{name: "capacity multiplication overflow", opts: []Option{WithQueueSize(^uint64(0) / 2), WithMaxPage(3)}, invalid: true},
			{name: "maximum uint64 queue size", opts: []Option{WithQueueSize(^uint64(0))}, invalid: true},
			{name: "maximum uint64 pages", opts: []Option{WithMaxPage(^uint64(0))}, invalid: true},
			{name: "unknown queue type", opts: []Option{WithQueueType(QueueType("unknown"))}, invalid: true},
			{name: "nil encoder", opts: []Option{WithEncoder(nil)}, invalid: true},
			{name: "nil decoder", opts: []Option{WithDecoder(nil)}, invalid: true},
			{name: "minimum group size", opts: []Option{WithGroupSize(1)}, groupOnly: true},
			{name: "zero group size", opts: []Option{WithGroupSize(0)}, invalid: true, groupOnly: true},
			{name: "negative group size", opts: []Option{WithGroupSize(-1)}, invalid: true, groupOnly: true},
		} {
			if tc.groupOnly && !grouped {
				continue
			}
			t.Run(fmt.Sprintf("group=%t/%s", grouped, tc.name), func(t *testing.T) {
				dir := t.TempDir()
				marker := filepath.Join(dir, "retained-data")
				if err := os.WriteFile(marker, []byte("retain"), 0600); err != nil {
					t.Fatal(err)
				}
				before := diskSnapshot(t, dir)
				opts := append([]Option{WithFileDir(dir)}, tc.opts...)
				var receiver boundaryReceiver
				var err error
				if grouped {
					var group *GroupQueue[int]
					group, err = NewGroupQueue[int]("g", opts...)
					if err == nil {
						receiver = group
						group.WaitInitialize()
					}
				} else {
					var queue *Queue[int]
					queue, err = NewQueue[int]("q", opts...)
					if err == nil {
						receiver = queue
						queue.WaitInitialize()
					}
				}
				if receiver != nil {
					defer receiver.CloseIndex()
					defer receiver.CloseQueue()
				}
				if tc.invalid {
					if !IsErrQueueOption(err) {
						t.Fatalf("invalid options: %v", err)
					}
					if len(diskSnapshot(t, dir)) != len(before) {
						t.Fatal("invalid configuration created queue files")
					}
				} else if err != nil {
					t.Fatalf("valid minimum rejected: %v", err)
				}
				data, readErr := os.ReadFile(marker)
				if readErr != nil || string(data) != "retain" {
					t.Fatal("configuration validation modified existing data")
				}
			})
		}
	}
}

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
