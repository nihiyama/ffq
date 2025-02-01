package main

import (
	"bytes"
	"fmt"
	"sync"
)

var queueBufPool = sync.Pool{
	New: func() any {
		// default 64kb Pool
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	},
}

func main() {
	buf1 := queueBufPool.Get().(*bytes.Buffer)
	buf2 := queueBufPool.Get().(*bytes.Buffer)
	buf1.WriteString("こんにちは")
	buf2.WriteString("さようなら")
	fmt.Println(buf1)
}
