package ffq

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateQueueDir(t *testing.T) {
	testcases := []struct {
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
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := createQueueDir(tc.input)
			if actual == nil {
				if tc.expect != "" {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tc.name, tc.expect, actual)
				}
			} else {
				if !strings.Contains(actual.Error(), tc.expect) {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tc.name, tc.expect, actual)
				}
			}

			if tc.afterRemove {
				os.RemoveAll(tc.input)
			}
		})
	}
}

func TestOpenIndexFile(t *testing.T) {
	testcases := []struct {
		name        string
		input       string
		expect      string
		afterRemove bool
	}{
		{
			name:        "directory already exists",
			input:       "testdata/utils/open_index_file/index",
			expect:      "",
			afterRemove: false,
		},
		{
			name:        "directory does not exist",
			input:       "testdata/utils/open_index_file/index",
			expect:      "",
			afterRemove: false,
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
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := createQueueDir(tc.input)
			if actual == nil {
				if tc.expect != "" {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tc.name, tc.expect, actual)
				}
			} else {
				if !strings.Contains(actual.Error(), tc.expect) {
					t.Fatalf("Failed test: %s, expect: %v, actual: %v", tc.name, tc.expect, actual)
				}
			}

			if tc.afterRemove {
				os.RemoveAll(tc.input)
			}
		})
	}
}

func TestReadIndex(t *testing.T) {
	dirName := "testdata/ffq"

	// Clean up before test
	defer os.RemoveAll(dirName)
	indexFilepath := filepath.Join(dirName, "index")
	createQueueDir(dirName)

	// Test reading from a non-existing file
	index, err := readIndex(indexFilepath)
	if err != nil {
		t.Fatalf("Failed to read index from a non-existing file: %v", err)
	}
	if index != 0 {
		t.Errorf("Expected index to be 0, got %d", index)
	}

	// Create and write an index to the file
	expectedIndex := uint64(12345)
	file, err := os.OpenFile(indexFilepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Failed to create test index file: %v", err)
	}
	_, err = fmt.Fprintf(file, "%019d", expectedIndex)
	if err != nil {
		t.Fatalf("Failed to write index to test index file: %v", err)
	}
	file.Close()

	// Test reading from an existing file
	index, err = readIndex(indexFilepath)
	if err != nil {
		t.Fatalf("Failed to read index from an existing file: %v", err)
	}
	if index != expectedIndex {
		t.Errorf("Expected index %d, got %d", expectedIndex, index)
	}
}
