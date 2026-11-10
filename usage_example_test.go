package ffq_test

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nihiyama/ffq"
)

func exampleCheck(err error) {
	if err != nil {
		panic(err)
	}
}

func ExampleQueue() {
	dir, err := os.MkdirTemp("", "ffq-example-")
	exampleCheck(err)
	defer os.RemoveAll(dir)
	q, err := ffq.NewQueue[int]("example", ffq.WithFileDir(dir))
	exampleCheck(err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { exampleCheck(q.CloseIndex()) }()
		for {
			m, err := q.Dequeue()
			if ffq.IsErrQueueClose(err) {
				return
			}
			exampleCheck(err)
			fmt.Println(*m.Item())
			exampleCheck(q.UpdateIndex(m))
		}
	}()
	exampleCheck(q.WaitInitializeWithError())
	for _, value := range []int{1, 2, 3} {
		v := value
		exampleCheck(q.Enqueue(&v))
	}
	exampleCheck(q.CloseQueue())
	<-done
	// Output:
	// 1
	// 2
	// 3
}

func ExampleQueue_BulkDequeue() {
	dir, err := os.MkdirTemp("", "ffq-example-")
	exampleCheck(err)
	defer os.RemoveAll(dir)
	q, err := ffq.NewQueue[int]("example", ffq.WithFileDir(dir), ffq.WithQueueSize(2048), ffq.WithMaxPage(3))
	exampleCheck(err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { exampleCheck(q.CloseIndex()) }()
		for {
			messages, err := q.BulkDequeue(50, 10*time.Millisecond)
			if ffq.IsErrQueueClose(err) {
				return
			}
			exampleCheck(err)
			for _, m := range messages {
				fmt.Println(*m.Item())
			}
			if len(messages) > 0 {
				exampleCheck(q.UpdateIndex(messages[len(messages)-1]))
			}
		}
	}()
	exampleCheck(q.WaitInitializeWithError())
	a, b, c := 1, 2, 3
	exampleCheck(q.BulkEnqueue([]*int{&a, &b, &c}))
	exampleCheck(q.CloseQueue())
	<-done
	// Output:
	// 1
	// 2
	// 3
}

func ExampleGroupQueue() {
	dir, err := os.MkdirTemp("", "ffq-example-")
	exampleCheck(err)
	defer os.RemoveAll(dir)
	gq, err := ffq.NewGroupQueue[int]("example", ffq.WithFileDir(dir))
	exampleCheck(err)
	done := make(chan struct{})
	count := 0
	go func() {
		defer close(done)
		defer func() { exampleCheck(gq.CloseIndex()) }()
		for {
			m, err := gq.Dequeue()
			if ffq.IsErrQueueClose(err) {
				return
			}
			exampleCheck(err)
			count++
			exampleCheck(gq.UpdateIndex(m))
		}
	}()
	exampleCheck(gq.WaitInitializeWithError())
	var producers sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		producers.Add(1)
		go func(name string) {
			defer producers.Done()
			for _, value := range []int{1, 2, 3} {
				v := value
				exampleCheck(gq.Enqueue(name, &v))
			}
		}(name)
	}
	producers.Wait()
	exampleCheck(gq.CloseQueue())
	<-done
	fmt.Println(count)
	// Output: 6
}

func ExampleGroupQueue_BulkDequeue() {
	dir, err := os.MkdirTemp("", "ffq-example-")
	exampleCheck(err)
	defer os.RemoveAll(dir)
	gq, err := ffq.NewGroupQueue[int]("example", ffq.WithFileDir(dir))
	exampleCheck(err)
	done := make(chan struct{})
	count := 0
	go func() {
		defer close(done)
		defer func() { exampleCheck(gq.CloseIndex()) }()
		for {
			messages, err := gq.BulkDequeue(50, 10*time.Millisecond)
			if ffq.IsErrQueueClose(err) {
				return
			}
			exampleCheck(err)
			last := make(map[string]*ffq.Message[int])
			for _, m := range messages {
				count++
				last[m.Name()] = m
			}
			for _, m := range last {
				exampleCheck(gq.UpdateIndex(m))
			}
		}
	}()
	exampleCheck(gq.WaitInitializeWithError())
	var producers sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		producers.Add(1)
		go func(name string) {
			defer producers.Done()
			a, b, c := 1, 2, 3
			exampleCheck(gq.BulkEnqueue(name, []*int{&a, &b, &c}))
		}(name)
	}
	producers.Wait()
	exampleCheck(gq.CloseQueue())
	<-done
	fmt.Println(count)
	// Output: 6
}
