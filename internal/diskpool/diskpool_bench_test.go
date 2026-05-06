package diskpool

import (
	"sync/atomic"
	"testing"
	"time"
)

// Models the receive path: a producer (the "peer goroutine") wants to hand
// off work to disk and immediately go back to reading the next message.
//
// The metric is per-handoff latency from the producer's perspective. With
// the pool, that's a channel send (≈microseconds) until the queue fills;
// without the pool, the producer is on the hook for the full disk latency
// every time.
//
// Run with -benchtime=Nx for a fixed number of submissions, or -benchtime=Ns
// to scale.
func BenchmarkSubmitVsSync(b *testing.B) {
	// Simulated per-chunk disk cost. 1ms is a reasonable spinning-disk
	// pwrite ballpark; the speedup grows with this number.
	const diskLatency = 1 * time.Millisecond

	work := func() {
		// busy-wait so the bench isn't dominated by Sleep granularity.
		end := time.Now().Add(diskLatency)
		for time.Now().Before(end) {
		}
	}

	b.Run("Sync", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			work()
		}
	})

	for _, workers := range []int{1, 4, 16} {
		b.Run("Pool/Workers="+itoa(workers), func(b *testing.B) {
			p := New(workers, 256)
			defer p.Close()
			var done atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.Submit(b.Context(), func() {
					work()
					done.Add(1)
				})
			}
			b.StopTimer()
			// Wait for all jobs to complete so the bench reports include
			// queueing but not setup of the next iteration.
			for done.Load() < int64(b.N) {
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
