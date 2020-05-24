package concurrency

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

// //////////////////////////////////////////////////////////
// A SafeInt64 is an int64 to be accessed safely with a lock. You should prefer AtomicInt64 instead
type SafeInt64 struct {
	mu  *sync.RWMutex
	val int64
}

func NewSafeInt64() *SafeInt64 {
	return &SafeInt64{mu: &sync.RWMutex{}}
}
func (s *SafeInt64) Get() int64 {
	s.mu.RLock()
	v := s.val
	s.mu.RUnlock()
	return v
}
func (s *SafeInt64) Add(v int64) int64 {
	s.mu.Lock()
	s.val += v
	newV := s.val
	s.mu.Unlock()
	return newV
}
func (s *SafeInt64) Set(v int64) {
	s.mu.Lock()
	s.val = v
	s.mu.Unlock()
}
func (i *SafeInt64) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// //////////////////////////////////////////////////////////
// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt64 struct {
	value int64
	_     [15]int64 // theres seems perf bumps on intel i7 at both 64 bytes (x2 perf), and 128 bytes (another x1.2), presumably different cache levels
}

func NewAtomicInt64() *AtomicInt64 {
	return &AtomicInt64{}
}
func (i *AtomicInt64) Add(n int64) int64 {
	return atomic.AddInt64(&i.value, n)
}
func (i *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&i.value)
}
func (i *AtomicInt64) Set(v int64) {
	atomic.StoreInt64(&i.value, v)
}
func (i *AtomicInt64) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

func (i *AtomicInt64) MarshalJSON() ([]byte, error) {
	bytes := []byte(strconv.FormatInt(i.Get(), 10))
	return bytes, nil
}

func (i *AtomicInt64) UnmarshalJSON(data []byte) error {
	value, err := strconv.ParseInt(string(data), 10, 64)
	if err == nil {
		atomic.StoreInt64(&i.value, value)
	}
	return err
}

// //////////////////////////////////////////////////////////

func SerialFor(iMax int, fn func(i int)) {
	for i := 0; i < iMax; i++ {
		fn(i)
	}
}

// ParallelForLimit:
// generally speaking this thing sucks except in trivial cases
// its does a single go func for each item, then blocks on a wait groupt
// until everything is done.
func ParallelFor(iMax int, fn func(i int)) {
	var wg sync.WaitGroup
	wg.Add(iMax)
	for i := 0; i < iMax; i++ {
		go func(n int) {
			fn(n)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// ParallelForLimit:
// Implement a sequential scan over an input array with multiple go workers
// each of which write uncontended changes to their own chunk of the output array
// in the event a worker finishes it does work stealing from the other workers
// (which does introduce contention), each worker has its own atomic counter
// to track its position in the input.
func ParallelForLimit(workerCount int, count int, fn func(i int)) {
	if workerCount == 1 {
		SerialFor(count, fn)
		return
	}

	if workerCount >= count {
		ParallelFor(count, fn)
		return
	}

	counterMax := workerCount * 16

	// should probably put everything in a pool
	workStealingArray := make([]int64, counterMax)
	workStealing := &workStealingArray[0] // the one contended write
	workersCanBeStolenFrom := make([]int64, counterMax)
	workersComplete := make([]int64, counterMax)
	counters := make([]int64, counterMax)
	countersMax := make([]int64, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	batchCount := int64(count / workerCount)
	batchMod := int(count % workerCount)
	batchCountPlusOne := batchCount + 1

	// initialize the workers counters, each gets a cache coherent range
	counterOffset := 0
	offset := int64(0)
	for i := 0; i < workerCount; i++ {
		counter := &counters[counterOffset]
		atomic.StoreInt64(counter, offset-1) // increments then checks, so -1 will become 0 for first access etc
		if i < batchMod {
			offset += batchCountPlusOne
		} else {
			offset += batchCount
		}
		countersMax[i] = offset
		counterOffset += 16
	}

	// all further access must be via atomic ops
	counterOffset = 0
	for i := 0; i < workerCount; i++ {
		go func(workerNum int, counterNum int) {
			workerIndex := workerNum
			counterIndex := counterNum

			counter := &counters[counterIndex]
			jMax := countersMax[workerIndex]

			var thisBatchCount int64
			if workerNum < batchMod {
				thisBatchCount = batchCountPlusOne
			} else {
				thisBatchCount = batchCount
			}

			// start with our own counter, do our batch each worker has a cache coherent chunk this way
			// reserve all work for us initially, switch to atomic counter 1 at a time once any thread
			// is looking to work steal
			j := atomic.AddInt64(counter, thisBatchCount) - thisBatchCount + 1
			for ; j < jMax; j++ {
				fn(int(j))
				if atomic.LoadInt64(workStealing) > 0 {
					atomic.StoreInt64(counter, j)
					break
				}
			}

			// yield before attempting to do anything with workstealing
			// arguably should probably sleep or something to introduce an even bigger delay
			// before switching to work stealing, its realy only needed for edge cases
			// and the rest of the time it really slows things down by introducing contention
			runtime.Gosched()

			// after we're done our batch, signal all workers to switch to work stealing mode
			if atomic.LoadInt64(workStealing) == 0 {
				atomic.StoreInt64(workStealing, 1)
			}
			// signal so other people can grab one from us
			atomic.StoreInt64(&workersCanBeStolenFrom[counterIndex], 1)

			var stoleSomething bool
			for {
				stoleSomething = false
				// check every worker job for work to steal
				// give up if we cant steal anything
				for k := 0; k < workerCount; k++ {
					// increment counter/worker indices the *16 is to avoid false sharing
					// we preincrement so that the first time we fall through to here we
					// advance to the next batch before searching for more work
					counterIndex += 16
					if counterIndex >= counterMax {
						counterIndex = 0
					}
					workerIndex += 1
					if workerIndex >= workerCount {
						workerIndex = 0
					}

					if atomic.LoadInt64(&workersComplete[counterIndex]) == 1 {
						continue
					}

					if atomic.LoadInt64(&workersCanBeStolenFrom[counterIndex]) == 0 {
						// cant know if we could steal something form this worker yet
						// assume we will be able to shortly, so dont give up
						stoleSomething = true
						continue
					}

					counter := &counters[counterIndex]
					jMax := countersMax[workerIndex]

					// this is slower than the above loop because atomic increment is slower than atomic load + register increment
					for j := atomic.AddInt64(counter, 1); j < jMax; j = atomic.AddInt64(counter, 1) {
						fn(int(j))
						stoleSomething = true
					}

					atomic.StoreInt64(&workersComplete[counterIndex], 1)
				}
				if !stoleSomething {
					break
				}
			}
			wg.Done()
		}(i, counterOffset)
		counterOffset += 16
	}
	wg.Wait()
}

type Pool struct {
	AllocationCount *AtomicInt64
	pool            sync.Pool
	new             func() interface{}
	reset           func(object interface{})
}

type PoolInfo struct {
	New   func() interface{}
	Reset func(object interface{})
}

func NewPool(p *PoolInfo) *Pool {
	allocCount := NewAtomicInt64()
	return &Pool{
		AllocationCount: allocCount,
		pool: sync.Pool{
			New: func() interface{} {
				allocCount.Add(1)
				return p.New()
			},
		},
		new:   p.New,
		reset: p.Reset,
	}
}

func (p *Pool) Get() interface{} { return p.pool.Get() }

func (p *Pool) Put(o interface{}) {
	p.reset(o)
	p.pool.Put(o)
}
