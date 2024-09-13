package ffq

import (
	"os"
	"strings"
	"testing"
)

func TestCreateQueueDir(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expect      string
		afterRemove bool
	}{
		{
			name:        "directory already exists",
			input:       "testdata/utils/create_queue_dir/ffq",
			expect:      "",
			afterRemove: false,
		},
		{
			name:        "directory does not exist",
			input:       "testdata/utils/create_queue_dir/ffq_not_exist",
			expect:      "",
			afterRemove: true,
		},
		{
			name:        "invalid directory name, stat error",
			input:       string([]byte{0x00}),
			expect:      "invalid argument",
			afterRemove: false,
		},
		{
			name:        "root manage directory, mkdir error",
			input:       "/invalid_dir",
			expect:      "permission denied",
			afterRemove: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := createQueueDir(tt.input)
			if tt.afterRemove {
				defer os.RemoveAll(tt.input)
			}
			if actual == nil {
				if tt.expect != "" {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tt.name, tt.expect, actual)
				}
			} else {
				if !strings.Contains(actual.Error(), tt.expect) {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tt.name, tt.expect, actual)
				}
			}
		})
	}
}

func TestOpenIndexFile(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedVal bool
		expectedErr string
		afterRemove bool
	}{
		{
			name:        "index already exists",
			input:       "testdata/utils/open_index_file/ffq/index",
			expectedVal: true,
			expectedErr: "",
			afterRemove: false,
		},
		{
			name:        "index not exists",
			input:       "testdata/utils/open_index_file/ffq/index_new",
			expectedVal: true,
			expectedErr: "",
			afterRemove: true,
		},
		{
			name:        "index cannot create",
			input:       "/root/index",
			expectedVal: false,
			expectedErr: "permission denied",
			afterRemove: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualVal, actualErr := openIndexFile(tt.input)
			if actualVal != nil {
				if !tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			} else {
				if tt.expectedVal {
					t.Fatalf("Failed test: %s, expectedVal: %v, actualVal: %v", tt.name, tt.expectedVal, actualVal)
				}
			}
			if actualErr == nil {
				if tt.expectedErr != "" {
					t.Fatalf("Failed test: %s, expectedErr: %v, actualErr: %v", tt.name, tt.expectedErr, actualErr)
				}
			} else {
				if !strings.Contains(actualErr.Error(), tt.expectedErr) {
					t.Fatalf("Failed test: %s, expectedErr: %v, actualErr: %v", tt.name, tt.expectedErr, actualErr)
				}
			}

			if tt.afterRemove {
				os.RemoveAll(tt.input)
			}
		})
	}
}

func TestReadIndex(t *testing.T) {
	tests := []struct {
		name                   string
		input                  string
		expectedPage           int
		expectedGlobalIndexVal int
		expectedLocalIndexVal  int
		expectedErr            string
	}{
		{
			name:                   "file does not exist",
			input:                  "testdata/utils/read_index/ffq/index_new",
			expectedPage:           0,
			expectedGlobalIndexVal: 0,
			expectedLocalIndexVal:  0,
			expectedErr:            "",
		},
		{
			name:                   "file cannnot open invalid permission",
			input:                  "testdata/utils/read_index/ffq/index_invalid_permission",
			expectedPage:           0,
			expectedGlobalIndexVal: 0,
			expectedLocalIndexVal:  0,
			expectedErr:            "permission denied",
		},
		{
			name:                   "read from file",
			input:                  "testdata/utils/read_index/ffq/index",
			expectedPage:           0,
			expectedGlobalIndexVal: 12345,
			expectedLocalIndexVal:  54321,
			expectedErr:            "",
		},
		{
			name:                   "read from invalid short data",
			input:                  "testdata/utils/read_index/ffq/index_invalid_eof",
			expectedPage:           0,
			expectedGlobalIndexVal: 0,
			expectedLocalIndexVal:  0,
			expectedErr:            "EOF",
		},
		{
			name:                   "read from invalid string data",
			input:                  "testdata/utils/read_index/ffq/index_invalid_string",
			expectedPage:           0,
			expectedGlobalIndexVal: 0,
			expectedLocalIndexVal:  0,
			expectedErr:            "unexpected EOF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualPage, actualGlobalIndexVal, actualLocalIndexVal, actualErr := readIndex(tt.input)
			if actualPage != tt.expectedPage {
				t.Fatalf("Failed test: %s, expectedSeekEnd: %v, actualSeekEnd: %v", tt.name, tt.expectedGlobalIndexVal, actualGlobalIndexVal)
			}
			if actualGlobalIndexVal != tt.expectedGlobalIndexVal {
				t.Fatalf("Failed test: %s, expectedGlobalIndexVal: %v, actualGlobalIndexVal: %v", tt.name, tt.expectedGlobalIndexVal, actualGlobalIndexVal)
			}
			if actualLocalIndexVal != tt.expectedLocalIndexVal {
				t.Fatalf("Failed test: %s, expectedLocalIndexVal: %v, actualLocalIndexVal: %v", tt.name, tt.expectedLocalIndexVal, actualLocalIndexVal)
			}
			if actualErr == nil {
				if tt.expectedErr != "" {
					t.Fatalf("Failed test: %s, expectedErr: %v, actualErr: %v", tt.name, tt.expectedErr, actualErr)
				}
			} else {
				if !strings.Contains(actualErr.Error(), tt.expectedErr) {
					t.Fatalf("Failed test: %s, expectedErr: %v, actualErr: %v", tt.name, tt.expectedErr, actualErr)
				}
			}
		})
	}
}
