package dreaming

import "sync"

// runBounded invokes fn(i) for each i in [0, n) with at most `concurrency`
// invocations in flight at once, blocking until all complete. fn must be safe
// for concurrent execution. A concurrency below 1 is treated as 1, so callers
// can pass a resolved setting directly.
//
// Dream phases use this to fan their per-item LLM/embedding calls out across
// the provider while keeping the surrounding budget checks and result
// application serial. The shared *TokenBudget is concurrency-safe, so parallel
// Spend is correct; the only effect of fan-out is a bounded over-spend of at
// most concurrency*PerCallCap past a cap, which the budget design tolerates.
func runBounded(concurrency, n int, fn func(i int)) {
	if concurrency < 1 {
		concurrency = 1
	}
	if n <= 0 {
		return
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer func() { <-sem; wg.Done() }()
			fn(i)
		}(i)
	}
	wg.Wait()
}
