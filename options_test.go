package ffq

import (
	"strings"
	"testing"
)

func TestWithFileDir(t *testing.T) {
	input := "/tmp"
	var expectRet error = nil
	expectedVal := input

	var options options
	f := WithFileDir(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.fileDir
	if *actualVal != expectedVal {
		t.Fatalf("Failed test: expectedVal: %v, actualVal: %v", expectedVal, actualVal)
	}
}

func TestWithQueueSize(t *testing.T) {
	tests := []struct {
		name        string
		input       int
		expectRet   string
		expectedVal int
	}{
		{
			name:        "queue size can set",
			input:       16,
			expectRet:   "",
			expectedVal: 16,
		},
		{
			name:        "queue size cannot set with less than 1",
			input:       0,
			expectRet:   "queueSize must be set to greater than 0",
			expectedVal: 0, // nil
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithQueueSize(tt.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tt.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tt.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			}
			actualVal := options.queueSize
			if actualVal == nil {
				if tt.expectedVal != 0 {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			} else {
				if *actualVal != tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			}
		})
	}
}

func TestWithEnqueueWriteSize(t *testing.T) {
	tests := []struct {
		name        string
		input       int
		expectRet   string
		expectedVal int
	}{
		{
			name:        "enqueue write size can set",
			input:       20,
			expectRet:   "",
			expectedVal: 20,
		},
		{
			name:        "enqueue write size cannot set with less than 1",
			input:       0,
			expectRet:   "enqueueWriteSize must be set to greater than 0",
			expectedVal: 0, // nil
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithEnqueueWriteSize(tt.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tt.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tt.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			}
			actualVal := options.enqueueWriteSize
			if actualVal == nil {
				if tt.expectedVal != 0 {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			} else {
				if *actualVal != tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			}
		})
	}
}

func TestWithPageSize(t *testing.T) {
	tests := []struct {
		name        string
		input       int
		expectRet   string
		expectedVal int
	}{
		{
			name:        "page size can set",
			input:       4,
			expectRet:   "",
			expectedVal: 4,
		},
		{
			name:        "page size cannot set with less than 2",
			input:       1,
			expectRet:   "pageSize must be set to greater than 1",
			expectedVal: 0, // nil
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithPageSize(tt.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tt.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tt.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			}
			actualVal := options.pageSize
			if actualVal == nil {
				if tt.expectedVal != 0 {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			} else {
				if *actualVal != tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			}
		})
	}
}

func TestWithDataFixedLength(t *testing.T) {
	tests := []struct {
		name        string
		input       uint64
		expectRet   string
		expectedVal uint64
	}{
		{
			name:        "data fixed length can set",
			input:       4,
			expectRet:   "",
			expectedVal: 4,
		},
		{
			name:        "data fixed length cannot set with less than 1",
			input:       0,
			expectRet:   "dataFixedLength must be set to greater than 0",
			expectedVal: 0, // nil
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var options options
			f := WithDataFixedLength(tt.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tt.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tt.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tt.name, tt.expectRet, actualRet)
				}
			}
			actualVal := options.dataFixedLength
			if actualVal == nil {
				if tt.expectedVal != 0 {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			} else {
				if *actualVal != tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			}
		})
	}
}

func TestWithJSONEncoder(t *testing.T) {
	var input func(v any) ([]byte, error) = func(v any) ([]byte, error) { return []byte{0x00}, nil }
	var expectRet error = nil
	expectedVal := input

	var options options
	f := WithJSONEncoder(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.jsonEncoder
	if actualVal == nil {
		t.Fatalf("Failed test: expectedVal: %p, actualVal: %p", expectedVal, actualVal)
	}
}

func TestWithJSONDecoder(t *testing.T) {
	var input func(data []byte, v any) error = func(data []byte, v any) error { return nil }
	var expectRet error = nil
	expectedVal := input

	var options options
	f := WithJSONDecoder(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.jsonDecoder
	if actualVal == nil {
		t.Fatalf("Failed test: expectedVal: %p, actualVal: %p", expectedVal, actualVal)
	}
}
