package diskpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-quicktest/qt"
)

func TestSubmitRunsToCompletion(t *testing.T) {
	t.Parallel()
	p := New(4, 16)
	defer p.Close()

	const n = 200
	var done atomic.Int64
	for i := 0; i < n; i++ {
		ok := p.Submit(t.Context(), func() {
			done.Add(1)
		})
		qt.Assert(t, qt.IsTrue(ok))
	}
	p.Close()
	qt.Assert(t, qt.Equals(done.Load(), int64(n)))
}

func TestSubmitBlocksWhenQueueFull(t *testing.T) {
	t.Parallel()
	// One worker, queue depth 1: with one job in flight and one queued,
	// the third Submit should block until something drains.
	p := New(1, 1)
	defer p.Close()

	gate := make(chan struct{})
	qt.Assert(t, qt.IsTrue(p.Submit(t.Context(), func() { <-gate })))
	qt.Assert(t, qt.IsTrue(p.Submit(t.Context(), func() {})))

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	ok := p.Submit(ctx, func() {})
	qt.Assert(t, qt.IsFalse(ok))

	close(gate) // let the pool drain
}

func TestSubmitAfterCloseFails(t *testing.T) {
	t.Parallel()
	p := New(2, 4)
	p.Close()
	qt.Assert(t, qt.IsFalse(p.Submit(t.Context(), func() {})))
}

func TestCloseDrainsQueuedJobs(t *testing.T) {
	t.Parallel()
	p := New(1, 64)
	const n = 50
	var done atomic.Int64
	for i := 0; i < n; i++ {
		p.Submit(t.Context(), func() { done.Add(1) })
	}
	p.Close()
	qt.Assert(t, qt.Equals(done.Load(), int64(n)))
}

func TestCloseUnblocksSubmit(t *testing.T) {
	t.Parallel()
	p := New(1, 1)
	gate := make(chan struct{})
	qt.Assert(t, qt.IsTrue(p.Submit(t.Context(), func() { <-gate })))
	qt.Assert(t, qt.IsTrue(p.Submit(t.Context(), func() {})))

	submitDone := make(chan bool, 1)
	go func() {
		submitDone <- p.Submit(t.Context(), func() {})
	}()

	// The third Submit is parked on the full queue. Closing the pool must
	// unblock it.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	p.Close()
	select {
	case ok := <-submitDone:
		// May go either way: if the worker drained before close, the third
		// Submit succeeded; otherwise it was unblocked by close. Both are
		// acceptable.
		_ = ok
	case <-time.After(time.Second):
		t.Fatal("Submit did not return")
	}
}
