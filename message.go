// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

// Message represents a generic message in the queue with an index and data.
//
// Fields:
//   - name: The name of the message in the queue.
//   - index: The index of the message in the queue.
//   - data: A pointer to the data associated with the message.
type Message[T any] struct {
	seekEnd     uint64
	globalIndex uint32
	localIndex  uint32
	name        string
	data        *T
}

func (m *Message[T]) Name() string {
	return m.name
}

// Index returns the index of the message in the queue.
//
// Returns:
//   - int: The index of the message.
//
// Example:
//
//	message := Message[string]{index: 1, data: &"exampleData"}
//	fmt.Println("Message index:", message.Index())
func (m *Message[T]) Index() int {
	return int(m.globalIndex)
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
