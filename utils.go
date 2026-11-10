// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func createQueueDir(dirName string) error {
	if _, err := os.Stat(dirName); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(dirName, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func openIndexFile(indexFilepath string) (*os.File, error) {
	indexFile, err := os.OpenFile(indexFilepath, fOpenFlag, 0644)
	if err != nil {
		return nil, err
	}
	return indexFile, err
}

func readIndex(path string) *uint64 { value, _ := loadQueueIndex(path); return value }

func loadQueueIndex(path string) (*uint64, error) {
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("initialize queue: cannot inspect index")
	}
	if !stat.Mode().IsRegular() || (stat.Size() != 0 && stat.Size() != 8) {
		return nil, fmt.Errorf("initialize queue: invalid index length or file type")
	}
	if stat.Size() == 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("initialize queue: cannot read index")
	}
	defer f.Close()
	var b [8]byte
	if _, err := io.ReadFull(f, b[:]); err != nil {
		return nil, fmt.Errorf("initialize queue: cannot read index")
	}
	value := binary.LittleEndian.Uint64(b[:])
	return &value, nil
}
