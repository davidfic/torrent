package torrent

import (
	"testing"
	"time"

	"github.com/go-quicktest/qt"
)

func TestUpdateRttEstimateEMA(t *testing.T) {
	t.Parallel()
	p := &Peer{}
	// First sample seeds the EMA.
	p.updateRttEstimate(100 * time.Millisecond)
	qt.Assert(t, qt.Equals(p.rttEstimate, 100*time.Millisecond))

	// Subsequent samples blend with alpha=0.125. Drift toward 200ms.
	for i := 0; i < 100; i++ {
		p.updateRttEstimate(200 * time.Millisecond)
	}
	// After many samples we should be very close to 200ms.
	if d := p.rttEstimate - 200*time.Millisecond; d > 1*time.Millisecond || d < -1*time.Millisecond {
		t.Fatalf("rttEstimate=%v, want ~200ms", p.rttEstimate)
	}

	// Zero/negative samples are ignored.
	before := p.rttEstimate
	p.updateRttEstimate(0)
	p.updateRttEstimate(-5 * time.Millisecond)
	qt.Assert(t, qt.Equals(p.rttEstimate, before))
}

func TestAdaptiveRequestCeiling(t *testing.T) {
	t.Parallel()
	// Construct enough of a Peer/Torrent to feed adaptiveRequestCeiling.
	tr := &Torrent{}
	tr.chunkSize = 16 << 10 // 16 KiB
	p := &Peer{t: tr}

	// No data → fallback signaled.
	_, ok := p.adaptiveRequestCeiling(4, 256)
	qt.Assert(t, qt.IsFalse(ok))

	// Set RTT and a non-zero rate. With rate=1MB/s and rtt=100ms,
	// BDP=100KB; chunkSize=16KB → target ≈ 6.
	p.rttEstimate = 100 * time.Millisecond
	// Rate is computed from _stats / totalExpectingTime; we shortcut via
	// the field directly. Set BytesReadUsefulData and the timing windows
	// to produce a known rate.
	p._stats.BytesReadUsefulData.Add(1 << 20) // 1 MiB
	now := time.Now()
	p.lastStartedExpectingToReceiveChunks = now.Add(-time.Second)
	p.cumulativeExpectedToReceiveChunks = 0
	// downloadRate = bytes / totalExpectingTime; with 1MiB over ~1s ≈ 1MB/s.

	target, ok := p.adaptiveRequestCeiling(4, 256)
	qt.Assert(t, qt.IsTrue(ok))
	if target < 5 || target > 8 {
		t.Fatalf("target=%d, want ~6 for BDP/(16KiB)", target)
	}

	// Floor and ceiling clamp.
	target, ok = p.adaptiveRequestCeiling(64, 256)
	qt.Assert(t, qt.IsTrue(ok))
	qt.Assert(t, qt.Equals(target, 64))

	target, ok = p.adaptiveRequestCeiling(0, 4)
	qt.Assert(t, qt.IsTrue(ok))
	qt.Assert(t, qt.Equals(target, 4))
}
