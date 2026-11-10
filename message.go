// Package ffq provides a file-based FIFO queue implementation that supports generic types.
package ffq

// Message represents an item in the queue with its associated metadata.
// It contains information about the page, index, queue name, and the item itself.
type Message[T any] struct {
	index    uint64    // index of the message in the queue.
	sequence uint64    // Absolute storage position; Index retains its circular semantics.
	owner    *Queue[T] // Reject progress updates from another instance or an old open.
	item     *T        // The actual data item contained in the message.
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
	if m.owner == nil {
		return ""
	}
	return m.owner.name
}

// Index returns the index of the message in the queue.
//
// Returns:
//   - uint64: The index of the message.
//
// Example:
//
//	index := message.Index()
//	fmt.Printf("Index: %d\n", index)
func (m *Message[T]) Index() uint64 {
	return m.index
}

// Item returns the actual data contained in the message.
//
// Returns:
//   - *T: A pointer to the data stored in the message.
//
// Example:
//
//	data := message.Item()
//	fmt.Println("Message data:", data)
func (m *Message[T]) Item() *T {
	return m.item
}
