// Package ffq provides a File-based FIFO Queue implementation that supports generic types.
package ffq

import "os"

var (
	// fOpenFlag is used when opening an existing queue file for read/write operations.
	// By default, it is set to os.O_RDWR | os.O_CREATE, which opens the file for reading and writing,
	// and creates it if it does not exist.
	fOpenFlag int = os.O_RDWR | os.O_CREATE

	// fCreateFlag is used when creating or truncating a queue file.
	// By default, it is set to os.O_RDWR | os.O_CREATE | os.O_TRUNC, which opens the file for reading and writing,
	// creates it if it does not exist, and truncates it if it does exist.
	fCreateFlag = os.O_RDWR | os.O_CREATE | os.O_TRUNC
)

// SetFSync modifies the file open flags to enable OS-level file synchronization (fsync).
// When SetFSync is called, both fOpenFlag and fCreateFlag are updated to include the os.O_SYNC flag.
// This ensures that file writes are flushed immediately to the underlying storage, improving data durability
// at the expense of write performance.
//
// Example:
//
//	// Enable fsync for all queue file operations
//	ffq.SetFSync()
//
//	// Subsequent file open operations will use the updated flags that enforce synchronization.
func SetFSync() {
	fOpenFlag = os.O_RDWR | os.O_CREATE | os.O_SYNC
	fCreateFlag = os.O_RDWR | os.O_CREATE | os.O_TRUNC | os.O_SYNC
}
