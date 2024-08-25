package ffq

import (
	"strings"
	"testing"
)

func TestWithFileDir(t *testing.T) {
	input := "/tmp"
	var expectRet error = nil
	expectVal := input

	var options options
	f := WithFileDir(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.fileDir
	if *actualVal != expectVal {
		t.Fatalf("Failed test: expectVal: %v, actualVal: %v", expectVal, actualVal)
	}
}

func TestWithQueueSize(t *testing.T) {
	testcases := []struct {
		name      string
		input     uint64
		expectRet string
		expectVal uint64
	}{
		{
			name:      "queue size can set",
			input:     16,
			expectRet: "",
			expectVal: 16,
		},
		{
			name:      "queue size cannot set with less than 1",
			input:     0,
			expectRet: "queueSize must be set to greater than 0",
			expectVal: 0, // nil
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var options options
			f := WithQueueSize(tc.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tc.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tc.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			}
			actualVal := options.queueSize
			if actualVal == nil {
				if tc.expectVal != 0 {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			} else {
				if *actualVal != tc.expectVal {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			}
		})
	}
}

func TestWithEnqueueWriteSize(t *testing.T) {
	testcases := []struct {
		name      string
		input     int
		expectRet string
		expectVal int
	}{
		{
			name:      "enqueue write size can set",
			input:     20,
			expectRet: "",
			expectVal: 20,
		},
		{
			name:      "enqueue write size cannot set with less than 1",
			input:     0,
			expectRet: "enqueueWriteSize must be set to greater than 0",
			expectVal: 0, // nil
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var options options
			f := WithEnqueueWriteSize(tc.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tc.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tc.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			}
			actualVal := options.enqueueWriteSize
			if actualVal == nil {
				if tc.expectVal != 0 {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			} else {
				if *actualVal != tc.expectVal {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			}
		})
	}
}

func TestWithPageSize(t *testing.T) {
	testcases := []struct {
		name      string
		input     int
		expectRet string
		expectVal int
	}{
		{
			name:      "page size can set",
			input:     4,
			expectRet: "",
			expectVal: 4,
		},
		{
			name:      "page size cannot set with less than 2",
			input:     1,
			expectRet: "pageSize must be set to greater than 1",
			expectVal: 0, // nil
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var options options
			f := WithPageSize(tc.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tc.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tc.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			}
			actualVal := options.pageSize
			if actualVal == nil {
				if tc.expectVal != 0 {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			} else {
				if *actualVal != tc.expectVal {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			}
		})
	}
}

func TestWithDataFixedLength(t *testing.T) {
	testcases := []struct {
		name      string
		input     uint64
		expectRet string
		expectVal uint64
	}{
		{
			name:      "data fixed length can set",
			input:     4,
			expectRet: "",
			expectVal: 4,
		},
		{
			name:      "data fixed length cannot set with less than 1",
			input:     0,
			expectRet: "dataFixedLength must be set to greater than 0",
			expectVal: 0, // nil
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			var options options
			f := WithDataFixedLength(tc.input)
			actualRet := f(&options)
			if actualRet == nil {
				if tc.expectRet != "" {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			} else {
				if !strings.Contains(actualRet.Error(), tc.expectRet) {
					t.Fatalf("Failed test: %s, expectRet: %v, actualRet: %v", tc.name, tc.expectRet, actualRet)
				}
			}
			actualVal := options.dataFixedLength
			if actualVal == nil {
				if tc.expectVal != 0 {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			} else {
				if *actualVal != tc.expectVal {
					t.Fatalf("Failed test: %s, expectVal: %v, actualVal: %v", tc.name, tc.expectVal, actualVal)
				}
			}
		})
	}
}

func TestWithJSONEncoder(t *testing.T) {
	var input func(v any) ([]byte, error) = func(v any) ([]byte, error) { return []byte{0x00}, nil }
	var expectRet error = nil
	expectVal := input

	var options options
	f := WithJSONEncoder(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.jsonEncoder
	if actualVal == nil {
		t.Fatalf("Failed test: expectVal: %p, actualVal: %p", expectVal, actualVal)
	}
}

func TestWithJSONDecoder(t *testing.T) {
	var input func(data []byte, v any) error = func(data []byte, v any) error { return nil }
	var expectRet error = nil
	expectVal := input

	var options options
	f := WithJSONDecoder(input)
	actualRet := f(&options)
	if expectRet != actualRet {
		t.Fatalf("Failed test: expectRet: %v, actualRet: %v", expectRet, actualRet)
	}
	actualVal := options.jsonDecoder
	if actualVal == nil {
		t.Fatalf("Failed test: expectVal: %p, actualVal: %p", expectVal, actualVal)
	}
}
