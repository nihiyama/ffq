package ringbuffer

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func removeAll(dir string, t *testing.T) {
	if r := recover(); r != nil {
		t.Logf("panic occured, %v", r)
	}
	os.RemoveAll(dir)
}

func TestCreateQueueDir_alreadyExist(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	err := createQueueDir(dir)
	if err != nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}

func TestCreateQueueDir_notMkdirPermission(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	os.Chmod(dir, 0400)
	err := createQueueDir(filepath.Join(dir, "dir"))
	if err == nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}

func TestCreateQueueDir_notExist(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "dir")
	defer removeAll(dir, t)

	err := createQueueDir(dir)
	if err != nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}

func TestOpenIndexFile_alreadyExist(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	indexFile, _ := os.CreateTemp(dir, "")
	indexFile.Close()
	_, err := openIndexFile(indexFile.Name())
	if err != nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}

func TestOpenIndexFile_notPermission(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	indexFile, _ := os.CreateTemp(dir, "")
	indexFile.Close()
	os.Chmod(indexFile.Name(), 0200)

	_, err := openIndexFile(indexFile.Name())
	if err == nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}
func TestOpenIndexFile_notExist(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	_, err := openIndexFile(filepath.Join(dir, "index"))
	if err != nil {
		t.Errorf("failed test: got is not nil, %v", err)
	}
}

func TestReadIndex_alreadyExist(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	tmpFile, _ := os.CreateTemp(dir, "index")

	var index uint64 = 12345678
	buf := indexBufPool.Get().(*[8]byte)
	binary.LittleEndian.PutUint64((*buf)[0:8], index)
	tmpFile.Write((*buf)[:])
	tmpFile.Close()

	got := readIndex(tmpFile.Name())
	if *got != index {
		t.Errorf("failed test: got is not equal index, %d, %d", *got, index)
	}
}

func TestReadIndex_notExist(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	got := readIndex("index")
	if got != nil {
		t.Errorf("failed test: got is not nil, %d", *got)
	}
}

func TestReadIndex_notPermission(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	tmpFile, _ := os.CreateTemp(dir, "index")

	var index uint64 = 12345678
	buf := indexBufPool.Get().(*[8]byte)
	binary.LittleEndian.PutUint64((*buf)[0:8], index)
	tmpFile.Write((*buf)[:])
	tmpFile.Close()
	os.Chmod(tmpFile.Name(), 0200)

	got := readIndex(tmpFile.Name())
	if got != nil {
		t.Errorf("failed test: got is not nil, %v", *got)
	}
}

func TestReadIndex_notMatchPattern(t *testing.T) {
	dir, _ := os.MkdirTemp("", "ffqtest")
	defer removeAll(dir, t)

	tmpFile, _ := os.CreateTemp(dir, "index")

	tmpFile.Write([]byte("123456"))
	tmpFile.Close()

	got := readIndex(tmpFile.Name())
	if got != nil {
		t.Errorf("failed test: got is not nil, %v", *got)
	}
}
