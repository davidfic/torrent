// Package diskpool runs storage I/O on a bounded worker pool so that the
// peer's read goroutine doesn't block on disk syscalls.
package diskpool

import (
	"context"
	"sync"
	"sync/atomic"
)

// Pool is a fixed-size set of workers each draining a shared job queue.
// Submit blocks when the queue is full, providing flow control: peers
// reading faster than the disk drains pile up there.
type Pool struct {
	jobs chan func()
	done chan struct{}
	wg   sync.WaitGroup

	closed atomic.Bool
}

// New starts a pool with the given number of workers and queue depth. Both
// must be positive.
func New(workers, queueDepth int) *Pool {
	p := &Pool{
		jobs: make(chan func(), queueDepth),
		done: make(chan struct{}),
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.run()
	}
	return p
}

// Submit enqueues fn. It blocks while the queue is full. It returns false
// if the pool is closed or ctx is cancelled before submission.
func (p *Pool) Submit(ctx context.Context, fn func()) bool {
	if p.closed.Load() {
		return false
	}
	select {
	case p.jobs <- fn:
		return true
	case <-ctx.Done():
		return false
	case <-p.done:
		return false
	}
}

// Close stops accepting new work and waits for queued jobs and running
// workers to finish.
func (p *Pool) Close() {
	if p.closed.Swap(true) {
		return
	}
	close(p.done)
	p.wg.Wait()
}

func (p *Pool) run() {
	defer p.wg.Done()
	for {
		select {
		case fn := <-p.jobs:
			fn()
		case <-p.done:
			// Drain whatever is already queued so submitters that won the
			// closed.Load race still get to run.
			for {
				select {
				case fn := <-p.jobs:
					fn()
				default:
					return
				}
			}
		}
	}
}
