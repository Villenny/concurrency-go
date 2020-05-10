package concurrency

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"hash"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testParallelForLimit(iMax int, workerCount int, t *testing.T) {
	//fmt.Println("BEGIN testParallelForLimit:")
	input := make([]int, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = i
	}
	results := make([]int, iMax)

	ParallelForLimit(workerCount, iMax, func(n int) {
		//fmt.Printf("%v ", n)
		in := input[n]
		results[n] += in
	})

	for i := 0; i < iMax; i++ {
		assert.Equal(t, i, results[i])
	}
	//fmt.Printf("\nEND testParallelForLimit")
}

func TestParallelForLimit_1(t *testing.T)        { testParallelForLimit(13, 1, t) }
func TestParallelForLimit_2(t *testing.T)        { testParallelForLimit(13, 2, t) }
func TestParallelForLimit_13(t *testing.T)       { testParallelForLimit(13, 13, t) }
func TestParallelForLimit_cpucount(t *testing.T) { testParallelForLimit(1300, runtime.NumCPU(), t) }
func TestParallelForLimit_cpucount_big(t *testing.T) {
	testParallelForLimit(13001, runtime.NumCPU(), t)
}

func TestSafeInt64(t *testing.T) {
	t.Run("can set, get and add", func(t *testing.T) {
		v := NewSafeInt64()
		v.Set(34)
		assert.Equal(t, int64(34), v.Get())
		v.Add(1)
		assert.Equal(t, int64(35), v.Get())
		assert.Equal(t, "35", v.String())
	})
}

func TestAtomicInt64(t *testing.T) {
	t.Run("can set, get and add", func(t *testing.T) {
		v := NewAtomicInt64()
		v.Set(34)
		assert.Equal(t, int64(34), v.Get())
		v.Add(1)
		assert.Equal(t, int64(35), v.Get())
		assert.Equal(t, "35", v.String())
	})
	t.Run("can marshal and unmarshal", func(t *testing.T) {
		test := struct {
			V *AtomicInt64
		}{
			V: NewAtomicInt64(),
		}
		test.V.Set(34)

		bytes, err := json.Marshal(&test)
		assert.Nil(t, err)
		s := string(bytes)
		assert.Equal(t, `{"V":34}`, s)

		test2 := struct {
			V *AtomicInt64
		}{
			V: NewAtomicInt64(),
		}
		err = json.Unmarshal(bytes, &test2)
		assert.Nil(t, err)
		assert.Equal(t, int64(34), test2.V.Get())
	})
}

func BenchmarkFor_ParallelForLimit_SQRT_1(b *testing.B)  { benchmarkParallelForLimit_SQRT(b, 1) }
func BenchmarkFor_ParallelForLimit_SQRT_2(b *testing.B)  { benchmarkParallelForLimit_SQRT(b, 2) }
func BenchmarkFor_ParallelForLimit_SQRT_4(b *testing.B)  { benchmarkParallelForLimit_SQRT(b, 4) }
func BenchmarkFor_ParallelForLimit_SQRT_8(b *testing.B)  { benchmarkParallelForLimit_SQRT(b, 8) }
func BenchmarkFor_ParallelForLimit_SQRT_16(b *testing.B) { benchmarkParallelForLimit_SQRT(b, 16) }

// This clearly sucks, no point in it
// func BenchmarkParallelForLimit_2048(b *testing.B) { benchmarkParallelForLimit(b, 2048) }

func benchmarkParallelForLimit_SQRT(b *testing.B, width int) {
	iMax := b.N
	input := make([]int, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = i * i
	}
	results := make([]float64, iMax)

	b.ResetTimer()
	ParallelForLimit(width, iMax, func(n int) {
		in := input[n]
		results[n] = math.Sqrt(float64(in))
	})
}

func BenchmarkFor_ParallelFor_SQRT(b *testing.B) {
	iMax := b.N
	input := make([]int, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = i * i
	}
	results := make([]float64, iMax)

	b.ResetTimer()
	// naive one worker per job, terrible performance
	ParallelFor(iMax, func(n int) {
		in := input[n]
		results[n] = math.Sqrt(float64(in))
	})

}

/*
goos: windows
goarch: amd64
pkg: bitbucket.org/kidozteam/bidder-server/pkg/platform/concurrency
BenchmarkSerialFor_SQRT
BenchmarkSerialFor_SQRT-8   	228372157	         5.31 ns/op	       0 B/op	       0 allocs/op
PASS
*/
func BenchmarkFor_SerialFor_SQRT(b *testing.B) {
	iMax := b.N
	input := make([]int, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = i * i
	}
	results := make([]float64, iMax)

	b.ResetTimer()
	SerialFor(iMax, func(n int) {
		in := input[n]
		results[n] = math.Sqrt(float64(in))
	})

}

var bufPool sync.Pool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}
var sha256Pool = sync.Pool{New: func() interface{} { return sha256.New() }}

func Sha256String(s string, salt string) []byte {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.WriteString(s)
	buf.WriteString(salt)

	hasher := sha256Pool.Get().(hash.Hash)
	_, err := hasher.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
	hash := hasher.Sum(nil) // this single allocation is amazingly high overhead

	buf.Reset()
	hasher.Reset()
	bufPool.Put(buf)
	sha256Pool.Put(hasher)
	return hash
}

func BenchmarkFor_SerialFor_SHA256(b *testing.B) {
	iMax := b.N
	input := make([]string, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = strconv.Itoa(i)
	}
	results := make([][]byte, iMax)

	b.ResetTimer()
	SerialFor(iMax, func(n int) {
		in := input[n]
		results[n] = Sha256String(in, ":hello world a lazy dog jumps over the energetic fox blah blah etc and so forth")
	})
}

func BenchmarkFor_ParallelForLimit_SHA256(b *testing.B) {
	iMax := b.N
	input := make([]string, iMax)
	for i := 0; i < iMax; i++ {
		input[i] = strconv.Itoa(i)
	}
	results := make([][]byte, iMax)

	b.ResetTimer()
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		in := input[n]
		results[n] = Sha256String(in, ":hello world a lazy dog jumps over the energetic fox blah blah etc and so forth")
	})
}

func BenchmarkInt64_Add(b *testing.B) {
	counter1 := int64(0)
	counter2 := int64(0)

	iMax := b.N
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		counter1 += 1
		counter2 += 1
	})

	// doesnt actually work, due to lack of race safety
	// just here for comparison
}

func BenchmarkInt64_Get(b *testing.B) {
	counter1 := int64(1)
	counter2 := int64(1)

	iMax := b.N
	results := make([]int64, iMax)
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		results[n] = counter1
		results[n] = counter2
	})

	// doesnt actually work, due to lack of race safety
	// just here for comparison
}

func BenchmarkSafeInt64_Add(b *testing.B) {
	s := struct {
		counter1 *SafeInt64
		counter2 *SafeInt64
	}{
		counter1: NewSafeInt64(),
		counter2: NewSafeInt64(),
	}

	iMax := b.N
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		s.counter1.Add(1)
		s.counter2.Add(1)
	})
	assert.Equal(b, iMax, int(s.counter1.Get()))
	assert.Equal(b, iMax, int(s.counter2.Get()))
	//fmt.Printf("ran %v\n", iMax)
}

func BenchmarkSafeInt64_Get(b *testing.B) {
	s := struct {
		counter1 *SafeInt64
		counter2 *SafeInt64
	}{
		counter1: NewSafeInt64(),
		counter2: NewSafeInt64(),
	}
	TheStruct = s

	iMax := b.N
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		s.counter1.Get()
		s.counter2.Get()
	})
	assert.Equal(b, 0, int(s.counter1.Get()))
	assert.Equal(b, 0, int(s.counter2.Get()))
}

func BenchmarkAtomicInt64_Add(b *testing.B) {
	counter1 := NewAtomicInt64()
	counter2 := NewAtomicInt64()

	iMax := b.N
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		counter1.Add(1)
		counter2.Add(1)
	})
	assert.Equal(b, iMax, int(counter1.Get()))
	assert.Equal(b, iMax, int(counter2.Get()))
}

func BenchmarkAtomicInt64_Get(b *testing.B) {
	counter1 := NewAtomicInt64()
	counter2 := NewAtomicInt64()

	iMax := b.N
	ParallelForLimit(runtime.NumCPU(), iMax, func(n int) {
		counter1.Get()
		counter2.Get()
	})
	assert.Equal(b, 0, int(counter1.Get()))
	assert.Equal(b, 0, int(counter2.Get()))
}

var TheStruct interface{}

func Benchmark_Inc500x2_SafeInt64(b *testing.B) {
	s := struct {
		counter1 *SafeInt64
		counter2 *SafeInt64
	}{
		counter1: NewSafeInt64(),
		counter2: NewSafeInt64(),
	}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			s.counter1.Add(1)
			s.counter2.Add(1)
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Get500x2_Int64(b *testing.B) {
	s := struct {
		counter1 int64
		counter2 int64
	}{
		counter1: 1,
		counter2: 1,
	}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			ResultsInt64 += s.counter1
			ResultsInt64 += s.counter2
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Get500x2_SafeInt64(b *testing.B) {
	s := struct {
		counter1 *SafeInt64
		counter2 *SafeInt64
	}{
		counter1: NewSafeInt64(),
		counter2: NewSafeInt64(),
	}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			ResultsInt64 += s.counter1.Get()
			ResultsInt64 += s.counter2.Get()
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Inc500x2_AtomicInt64(b *testing.B) {
	s := struct {
		counter1 *AtomicInt64
		counter2 *AtomicInt64
	}{
		counter1: NewAtomicInt64(),
		counter2: NewAtomicInt64(),
	}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			s.counter1.Add(1)
			s.counter2.Add(1)
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

var ResultsInt64 int64

func Benchmark_Get500x2_AtomicInt64(b *testing.B) {
	s := struct {
		counter1 *AtomicInt64
		counter2 *AtomicInt64
	}{
		counter1: NewAtomicInt64(),
		counter2: NewAtomicInt64(),
	}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			ResultsInt64 += s.counter1.Get()
			ResultsInt64 += s.counter2.Get()
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Inc500x2_RawAtomic_falseSharing(b *testing.B) {
	s := struct {
		counter1 int64
		counter2 int64
	}{}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			atomic.AddInt64(&s.counter1, 1)
			atomic.AddInt64(&s.counter2, 1)
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Inc500x2_RawAtomic_noFalseSharing(b *testing.B) {
	s := struct {
		counter1 int64
		_        [15]int64
		counter2 int64
		_        [15]int64
	}{}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			atomic.AddInt64(&s.counter1, 1)
			atomic.AddInt64(&s.counter2, 1)
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

func Benchmark_Inc500x2_Int64_noFalseSharing(b *testing.B) {
	s := struct {
		counter1 int64
		_        [7]int64
		counter2 int64
		_        [7]int64
	}{}
	TheStruct = s

	iMax := b.N
	var wg sync.WaitGroup
	wg.Add(iMax)

	fn1 := func(n int) {
		for j := 0; j < 500; j++ {
			s.counter1 += 1
			s.counter2 += 1
		}
		wg.Done()
	}

	for i := 0; i < iMax; i++ {
		go fn1(i)
	}
	wg.Wait()
}

/*
goos: windows
goarch: amd64
pkg: github.com/villenny/concurrency-go
BenchmarkFor_ParallelForLimit_SQRT_1-8          	227508680	         5.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_ParallelForLimit_SQRT_2-8          	529180558	         5.68 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_ParallelForLimit_SQRT_4-8          	543552358	         2.39 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_ParallelForLimit_SQRT_8-8          	678688118	         8.51 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_ParallelForLimit_SQRT_16-8         	     100	  12596907 ns/op	     148 B/op	       0 allocs/op
BenchmarkFor_ParallelFor_SQRT-8                 	 4943403	       238 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_SerialFor_SQRT-8                   	340297232	         3.40 ns/op	       0 B/op	       0 allocs/op
BenchmarkFor_SerialFor_SHA256-8                 	 1720980	       698 ns/op	      32 B/op	       1 allocs/op
BenchmarkFor_ParallelForLimit_SHA256-8          	 5946825	       203 ns/op	      32 B/op	       1 allocs/op
BenchmarkInt64_Add-8                            	188283139	         6.59 ns/op	       0 B/op	       0 allocs/op
BenchmarkInt64_Get-8                            	488312643	         2.69 ns/op	       8 B/op	       0 allocs/op
BenchmarkSafeInt64_Add-8                        	 7602818	       158 ns/op	       0 B/op	       0 allocs/op
BenchmarkSafeInt64_Get-8                        	30885693	        41.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkAtomicInt64_Add-8                      	66738595	        17.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkAtomicInt64_Get-8                      	1000000000	         0.775 ns/op	       0 B/op	       0 allocs/op
Benchmark_Inc500x2_SafeInt64-8                  	   12219	    112004 ns/op	     174 B/op	       1 allocs/op
Benchmark_Get500x2_Int64-8                      	 3112036	       394 ns/op	      11 B/op	       0 allocs/op
Benchmark_Get500x2_SafeInt64-8                  	   59174	     22655 ns/op	       0 B/op	       0 allocs/op
Benchmark_Inc500x2_AtomicInt64-8                	  139681	      8611 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get500x2_AtomicInt64-8                	 1570255	       762 ns/op	      57 B/op	       0 allocs/op
Benchmark_Inc500x2_RawAtomic_falseSharing-8     	   55102	     21302 ns/op	       0 B/op	       0 allocs/op
Benchmark_Inc500x2_RawAtomic_noFalseSharing-8   	  134972	      8852 ns/op	       0 B/op	       0 allocs/op
Benchmark_Inc500x2_Int64_noFalseSharing-8       	  273004	      4581 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/villenny/concurrency-go	80.581s
Success: Benchmarks passed.
*/