package bench

import (
	"fmt"
	"strings"
)

var tests = []int{10, 100, 1000}

type BenchmarkData struct {
	Val1  string
	Val2  int
	Val3  []string
	Val4  map[string]string
	Val5  string
	Val6  int
	Val7  []string
	Val8  map[string]string
	Val9  string
	Val10 int
	Val11 []string
	Val12 map[string]string
}

func createData(n int) []*BenchmarkData {
	data := make([]*BenchmarkData, 0, n)
	for i := 0; i < n; i++ {
		val3 := make([]string, 0, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val4 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		val7 := make([]string, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val8 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		val11 := make([]string, 10)
		for j := 0; j < 10; j++ {
			val3 = append(val3, fmt.Sprintf("string silice val3, %d, %d", j, i))
		}
		val12 := make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			k := fmt.Sprintf("key%d", j)
			val4[k] = fmt.Sprintf("string map val4, %d, %d", j, i)
		}
		d := BenchmarkData{
			Val1:  fmt.Sprintf("string val1, %d, 1kb data: %s", i, strings.Repeat("a", 1024)),
			Val2:  i * 2,
			Val3:  val3,
			Val4:  val4,
			Val5:  fmt.Sprintf("string val5, %d", i),
			Val6:  i * 6,
			Val7:  val7,
			Val8:  val8,
			Val9:  fmt.Sprintf("string val9, %d", i),
			Val10: i * 10,
			Val11: val11,
			Val12: val12,
		}
		data = append(data, &d)
	}
	return data
}
