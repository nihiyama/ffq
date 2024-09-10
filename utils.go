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

func readIndex(indexFilepath string) (uint64, int, int, error) {
	var err error
	if _, err := os.Stat(indexFilepath); os.IsNotExist(err) {
		return 0, 0, 0, nil
	}
	indexFile, err := os.Open(indexFilepath)
	if err != nil {
		return 0, 0, 0, err
	}
	defer indexFile.Close()

	// uint64 size is 8, uint32 size is 4
	// | -- seekEnd(8) -- | -- globalIndex(4) -- | -- localIndex(4) -- |
	var seekEnd uint64
	var globalIndex uint32
	var localIndex uint32
	err = binary.Read(indexFile, binary.LittleEndian, &seekEnd)
	if err != nil {
		return 0, 0, 0, err
	}
	err = binary.Read(indexFile, binary.LittleEndian, &globalIndex)
	if err != nil {
		return seekEnd, 0, 0, err
	}
	err = binary.Read(indexFile, binary.LittleEndian, &localIndex)
	if err != nil {
		return seekEnd, int(globalIndex), 0, err
	}

	return seekEnd, int(globalIndex), int(localIndex), nil
}
