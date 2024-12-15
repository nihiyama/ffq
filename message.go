// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

// Message represents an item in the queue with its associated metadata.
// It contains information about the page, global index, local index, queue name, and the data itself.
type Message[T any] struct {
	page        int    // The page number where the message is stored.
	globalIndex int    // The global index of the message in the queue.
	localIndex  int    // The local index of the message within the page.
	name        string // The name of the queue to which the message belongs.
	data        *T     // The actual data of the message.
}

// Name returns the name of the queue to which the message belongs.
//
// Returns:
//   - string: The name of the queue.
//
// Example:
//
//	queueName := message.Name()
//	fmt.Println(queueName)
func (m *Message[T]) Name() string {
	return m.name
}

// Index returns the global and local indices of the message in the queue.
//
// Returns:
//   - (int, int): A tuple containing the global index and the local index.
//
// Example:
//
//	globalIdx, localIdx := message.Index()
//	fmt.Printf("Global Index: %d, Local Index: %d\n", globalIdx, localIdx)
func (m *Message[T]) Index() (int, int) {
	return m.globalIndex, m.localIndex
}

// Data returns the actual data contained in the message.
//
// Returns:
//   - *T: A pointer to the data stored in the message.
//
// Example:
//
//	data := message.Data()
//	fmt.Println("Message data:", data)
func (m *Message[T]) Data() *T {
	return m.data
}
