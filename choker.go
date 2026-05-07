package torrent

import (
	"math/rand"
	"sort"
	"time"
)

// ChokerConfig overrides defaults for the per-torrent upload-slot choker.
// Nil keeps legacy behaviour where every uploadAllowed() peer is unchoked.
type ChokerConfig struct {
	// Total unchoke slots, including OptimisticSlots.
	NumSlots int
	// Number of those slots reserved for the optimistic-unchoke rotation.
	OptimisticSlots int
	// Re-evaluate the slot membership at this interval.
	Interval time.Duration
	// Rotate the optimistic slot at this interval.
	OptimisticInterval time.Duration
	// When seeding, prefer peers far from 50% complete (round-robin
	// tie-breaker) instead of by recent contribution. Mirrors libtorrent's
	// anti-leech curve.
	SeedAntiLeech bool
}

func (c *ChokerConfig) numSlots() int {
	if c.NumSlots <= 0 {
		return 5
	}
	return c.NumSlots
}

func (c *ChokerConfig) optimisticSlots() int {
	if c.OptimisticSlots < 0 {
		return 0
	}
	if c.OptimisticSlots == 0 {
		return 1
	}
	return c.OptimisticSlots
}

func (c *ChokerConfig) interval() time.Duration {
	if c.Interval <= 0 {
		return 10 * time.Second
	}
	return c.Interval
}

func (c *ChokerConfig) optimisticInterval() time.Duration {
	if c.OptimisticInterval <= 0 {
		return 30 * time.Second
	}
	return c.OptimisticInterval
}

// runChoker is started as a goroutine per torrent when ChokerConfig is set.
// Exits when t.closed fires.
func (t *Torrent) runChoker(cfg *ChokerConfig) {
	state := &chokerState{}
	timer := time.NewTimer(cfg.interval())
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			t.chokerTick(cfg, state, time.Now())
			timer.Reset(cfg.interval())
		case <-t.closed.Done():
			return
		}
	}
}

type chokerState struct {
	currentOptimistic *PeerConn
	nextOptimistic    time.Time
}

// chokerTick recomputes the unchoke set. The lock is held only for the
// snapshot and the apply; the sort runs unlocked.
func (t *Torrent) chokerTick(cfg *ChokerConfig, state *chokerState, now time.Time) {
	t.cl.lock()
	peers := make([]*PeerConn, 0, len(t.conns))
	for pc := range t.conns {
		if !pc.peerInterested {
			continue
		}
		// Update per-peer recent-bytes counter for sort.
		cur := pc._stats.BytesReadData.Int64()
		pc.chokerRecentBytes = cur - pc.chokerLastBytesRead
		pc.chokerLastBytesRead = cur
		peers = append(peers, pc)
	}
	seeding := t.seeding()
	t.cl.unlock()

	// Sort outside the lock.
	if seeding && cfg.SeedAntiLeech {
		sort.SliceStable(peers, lessByAntiLeech(peers))
	} else {
		sort.SliceStable(peers, lessByDownloadContribution(peers))
	}

	regularSlots := cfg.numSlots() - cfg.optimisticSlots()
	if regularSlots < 0 {
		regularSlots = 0
	}
	unchokeSet := make(map[*PeerConn]struct{}, cfg.numSlots())
	for i, pc := range peers {
		if i >= regularSlots {
			break
		}
		unchokeSet[pc] = struct{}{}
	}

	// Optimistic rotation.
	if now.After(state.nextOptimistic) || state.currentOptimistic == nil {
		var pool []*PeerConn
		if regularSlots < len(peers) {
			pool = peers[regularSlots:]
		}
		if len(pool) > 0 {
			state.currentOptimistic = pool[rand.Intn(len(pool))]
		} else {
			state.currentOptimistic = nil
		}
		state.nextOptimistic = now.Add(cfg.optimisticInterval())
	}
	if state.currentOptimistic != nil {
		unchokeSet[state.currentOptimistic] = struct{}{}
	}

	t.cl.lock()
	defer t.cl.unlock()
	for pc := range t.conns {
		_, want := unchokeSet[pc]
		pc.chokerWantsChoked = !want
		if !want && !pc.choking {
			pc.choke(pc.write)
		}
		// Unchokes happen lazily inside upload() so we don't spam messages
		// to peers that don't have anything we'd send anyway.
	}
}

func lessByDownloadContribution(peers []*PeerConn) func(i, j int) bool {
	return func(i, j int) bool {
		return peers[i].chokerRecentBytes > peers[j].chokerRecentBytes
	}
}

// lessByAntiLeech ranks peers far from 50% complete first -- they need
// help most. Ties broken by recent contribution to keep things moving.
func lessByAntiLeech(peers []*PeerConn) func(i, j int) bool {
	return func(i, j int) bool {
		si := antiLeechScore(peers[i])
		sj := antiLeechScore(peers[j])
		if si != sj {
			return si > sj
		}
		return peers[i].chokerRecentBytes > peers[j].chokerRecentBytes
	}
}

func antiLeechScore(p *PeerConn) int {
	if all, _ := p.peerHasAllPieces(); all {
		// They have everything. No point uploading more.
		return -1
	}
	want := p.bestPeerNumPieces()
	if want == 0 {
		return 0
	}
	frac := float64(p.remotePieceCount()) / float64(want)
	switch {
	case frac >= 0.8:
		return 100 // about to seed; help them get there
	case frac < 0.2:
		return 50 // just starting; help them onto the swarm
	default:
		return 0
	}
}
