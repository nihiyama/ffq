package ffq

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The limit is process-local, so fault injection never affects other tests or
// the container. A regular-file write can partially succeed before hitting it.
func TestPartialQueueWriteRollback(t *testing.T) {
	if dir := os.Getenv("FFQ_PARTIAL_WRITE_HELPER"); dir != "" {
		q, err := NewQueue[string]("q", WithFileDir(dir), WithQueueSize(2))
		if err != nil {
			t.Fatal(err)
		}
		if err := q.waitInitialize(); err != nil {
			t.Fatal(err)
		}
		var original syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Fatal(err)
		}
		signal.Ignore(syscall.SIGXFSZ)
		limit := original
		limit.Cur = 64
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		value := strings.Repeat("dummy", 1000)
		writeErr := q.Enqueue(&value)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Fatal(err)
		}
		if writeErr == nil {
			t.Fatal("partial write was accepted")
		}
		info, err := os.Stat(filepath.Join(dir, "queue.0"))
		if err != nil || info.Size() != 0 {
			t.Fatal("partial record was not rolled back")
		}
		value = "ok"
		if err := q.Enqueue(&value); err != nil {
			t.Fatal(err)
		}
		q.CloseQueue()
		q.CloseIndex()
		q, err = NewQueue[string]("q", WithFileDir(dir), WithQueueSize(2))
		if err != nil {
			t.Fatal(err)
		}
		if err := q.waitInitialize(); err != nil {
			t.Fatal(err)
		}
		m, err := q.Dequeue()
		if err != nil || *m.Item() != "ok" {
			t.Fatal("retry did not produce a recoverable record")
		}
		if err := q.UpdateIndex(m); err != nil {
			t.Fatal(err)
		}
		q.CloseQueue()
		q.CloseIndex()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPartialQueueWriteRollback$")
	cmd.Env = append(os.Environ(), "FFQ_PARTIAL_WRITE_HELPER="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("partial write probe: %v %s", err, out)
	}
}

func TestPartialIndexWriteStopsProgress(t *testing.T) {
	if dir := os.Getenv("FFQ_PARTIAL_INDEX_HELPER"); dir != "" {
		q, err := NewQueue[int]("q", WithFileDir(dir))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { q.CloseQueue(); q.CloseIndex() }()
		if err := q.waitInitialize(); err != nil {
			t.Fatal(err)
		}
		value := 1
		if err := q.Enqueue(&value); err != nil {
			t.Fatal(err)
		}
		m, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		var original syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Fatal(err)
		}
		signal.Ignore(syscall.SIGXFSZ)
		limit := original
		limit.Cur = 4 // Permit only half of the eight-byte progress record.
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		writeErr := q.UpdateIndex(m)
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Fatal(err)
		}
		if !isIOFailure(writeErr) || q.ackNext.Load() != 0 {
			t.Fatalf("partial index advanced progress: %v, %d", writeErr, q.ackNext.Load())
		}
		if err := q.UpdateIndex(m); err != writeErr {
			t.Fatalf("failed queue accepted another update: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "index"))
		if err != nil || len(data) != 4 {
			t.Fatalf("partial index: %x, %v", data, err)
		}
		data, err = os.ReadFile(filepath.Join(dir, "queue.0"))
		if err != nil || string(data) != "[1]\n" {
			t.Fatalf("queue data changed after index failure: %q, %v", data, err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPartialIndexWriteStopsProgress$")
	cmd.Env = append(os.Environ(), "FFQ_PARTIAL_INDEX_HELPER="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("partial index write probe: %v %s", err, out)
	}
}
