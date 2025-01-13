// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ringbuffer

import (
	"encoding/binary"
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
	indexFile, err := os.OpenFile(indexFilepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return indexFile, err
}

func readIndex(indexFilepath string) *uint64 {
	var err error
	if _, err := os.Stat(indexFilepath); os.IsNotExist(err) {
		return nil
	}
	indexFile, err := os.Open(indexFilepath)
	if err != nil {
		// if cannot open remove index file
		os.Remove(indexFilepath)
		return nil
	}
	defer indexFile.Close()

	// uint64 size is 8
	// | -- index(8) -- |
	var index uint64
	err = binary.Read(indexFile, binary.LittleEndian, &index)
	if err != nil {
		// if cannot read remove index file
		os.Remove(indexFilepath)
		return nil
	}

	return &index
}
