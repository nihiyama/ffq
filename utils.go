// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

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

func readIndex(indexFilepath string) (int, int, int, error) {
	var err error
	if _, err := os.Stat(indexFilepath); os.IsNotExist(err) {
		return 0, -1, -1, nil
	}
	indexFile, err := os.Open(indexFilepath)
	if err != nil {
		return 0, -1, -1, err
	}
	defer indexFile.Close()

	// uint32 size is 4
	// | -- page(4) -- | -- globalIndex(4) -- | -- localIndex(4) -- |
	var page uint32
	var globalIndex uint32
	var localIndex uint32
	err = binary.Read(indexFile, binary.LittleEndian, &page)
	if err != nil {
		return 0, -1, -1, err
	}
	err = binary.Read(indexFile, binary.LittleEndian, &globalIndex)
	if err != nil {
		return int(page), -1, -1, err
	}
	err = binary.Read(indexFile, binary.LittleEndian, &localIndex)
	if err != nil {
		return int(page), int(globalIndex), -1, err
	}

	return int(page), int(globalIndex), int(localIndex), nil
}
