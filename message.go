// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

// Message represents a generic message in the queue with an index and data.
//
// Fields:
//   - index: The index of the message in the queue.
//   - data: A pointer to the data associated with the message.
type Message[T any] struct {
	index uint64
	data  *T
}

// Index returns the index of the message in the queue.
//
// Returns:
//   - uint64: The index of the message.
//
// Example:
//
//	message := Message[string]{index: 1, data: &"exampleData"}
//	fmt.Println("Message index:", message.Index())
func (m *Message[T]) Index() uint64 {
	return m.index
}

// Data returns the data associated with the message.
//
// Returns:
//   - *T: A pointer to the data of the message.
//
// Example:
//
//	message := Message[string]{index: 1, data: &"exampleData"}
//	fmt.Println("Message data:", *message.Data())
func (m *Message[T]) Data() *T {
	return m.data
}
