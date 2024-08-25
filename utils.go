// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

import (
	"fmt"
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

func readIndex(indexFilepath string) (uint64, error) {
	var err error
	if _, err := os.Stat(indexFilepath); os.IsNotExist(err) {
		return 0, nil
	}
	indexFile, err := os.Open(indexFilepath)
	if err != nil {
		return 0, err
	}
	// uint64 size is 19
	buffer := make([]byte, 19)
	_, err = indexFile.Read(buffer)
	if err != nil {
		return 0, err
	}
	var index uint64
	_, err = fmt.Sscanf(string(buffer), "%d", &index)
	if err != nil {
		return 0, err
	}
	return index, nil
}
