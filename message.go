package ffq

type Message[T any] struct {
	index uint64
	data  *T
}

func (m *Message[T]) Index() uint64 {
	return m.index
}

func (m *Message[T]) Data() *T {
	return m.data
}
