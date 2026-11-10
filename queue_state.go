package ffq

import (
	"fmt"
)

type queueFailure struct{ err error }

func (q *Queue[T]) fail(err error) error {
	if q.failure.CompareAndSwap(nil, &queueFailure{err: err}) {
		close(q.failureSig)
		if q.group != nil {
			q.group.fail(err)
			q.group.signalEnqueue()
		}
	}
	return q.failure.Load().err
}

func (q *Queue[T]) failIO(operation string, err error) error {
	return q.fail(fmt.Errorf("queue I/O failed (%s); stop and reopen: %w", operation, err))
}

func (q *Queue[T]) currentFailure() error {
	if failure := q.failure.Load(); failure != nil {
		return failure.err
	}
	if q.group != nil {
		if failure := q.group.failure.Load(); failure != nil {
			return failure.err
		}
	}
	return nil
}

func (q *Queue[T]) canEnqueue() error {
	if !q.ready.Load() {
		if err := q.waitInitialize(); err != nil {
			return err
		}
	}
	if err := q.currentFailure(); err != nil {
		return err
	}
	if q.isQueueClosed.Load() {
		return ErrQueueClose
	}
	return nil
}

func (q *Queue[T]) publish(m *Message[T]) error {
	if err := q.currentFailure(); err != nil {
		return err
	}
	if q.isQueueClosed.Load() {
		return ErrQueueClose
	}
	// A ready send needs only the data channel's lock. Cancellation channels
	// participate in the select only when publication actually has to wait.
	select {
	case q.queue <- m:
		q.notifyReplay()
		return nil
	default:
	}
	select {
	case <-q.stop:
		return ErrQueueClose
	case <-q.failureSig:
		return q.failure.Load().err
	case <-q.parentFailure:
		return q.group.failure.Load().err
	case q.queue <- m:
		q.notifyReplay()
		return nil
	}
}

func (q *Queue[T]) notifyReplay() {
	// Normal group enqueues notify after each capacity-bounded chunk. Replay
	// must notify while initializing, before its buffer can fill and block.
	if q.group != nil && !q.ready.Load() {
		q.group.signalEnqueue()
	}
}

func (q *Queue[T]) received(m *Message[T], ok bool) (*Message[T], error) {
	if ok {
		return m, nil
	}
	q.isQueueClosedRecieved.Store(true)
	if err := q.currentFailure(); err != nil {
		return nil, err
	}
	return nil, ErrQueueClose
}

// Wait only at a page boundary. The consumer publishes progress after its
// index write succeeds. The SPSC fast path does not acquire a mutex.
func (q *Queue[T]) waitPage(page uint64) error {
	for q.pagePresent[page] && q.pageEnds[page] > q.ackNext.Load() {
		select {
		case <-q.progress:
		case <-q.stop:
			return ErrQueueClose
		case <-q.failureSig:
			return q.failure.Load().err
		case <-q.parentFailure:
			return q.group.failure.Load().err
		}
	}
	return nil
}

func (q *Queue[T]) waitInitialize() error { <-q.initializeBlock; return q.initErr }
