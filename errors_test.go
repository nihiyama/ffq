package ffq

import (
	"testing"
)

func TestIsErrQueueNotFound(t *testing.T) {
	err := ErrQueueNotFound
	expect := true
	actual := IsErrQueueNotFound(err)
	if expect != actual {
		t.Fatalf("Failed test, expect: %v, actual: %v", expect, actual)
	}
}

func TestIsErrQueueOption(t *testing.T) {
	err := ErrQueueOption
	expect := true
	actual := IsErrQueueOption(err)
	if expect != actual {
		t.Fatalf("Failed test, expect: %v, actual: %v", expect, actual)
	}
}

func TestIsErrQueueClose(t *testing.T) {
	err := ErrQueueClose
	expect := true
	actual := IsErrQueueClose(err)
	if expect != actual {
		t.Fatalf("Failed test, expect: %v, actual: %v", expect, actual)
	}
}
