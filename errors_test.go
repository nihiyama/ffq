package ffq

import (
	"testing"
)

func TestIsErrQueueNotFound(t *testing.T) {
	err := ErrQueueNotFound
	want := true
	got := IsErrQueueNotFound(err)
	if want != got {
		t.Errorf("failed test: want: %v, got: %v", want, got)
	}
}

func TestIsErrQueueOption(t *testing.T) {
	err := ErrQueueOption
	want := true
	got := IsErrQueueOption(err)
	if want != got {
		t.Errorf("failed test: want: %v, got: %v", want, got)
	}
}

func TestIsErrQueueClose(t *testing.T) {
	err := ErrQueueClose
	want := true
	got := IsErrQueueClose(err)
	if want != got {
		t.Errorf("failed test: want: %v, got: %v", want, got)
	}
}
