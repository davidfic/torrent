package torrent

import (
	"time"
)

// runSnubChecker periodically marks peers as "snubbed" -- having outstanding
// requests but no useful chunk for SnubTimeout -- and frees their requests
// for reassignment. Started per torrent when ClientConfig.SnubTimeout > 0.
func (t *Torrent) runSnubChecker(timeout time.Duration) {
	const tickInterval = 5 * time.Second
	timer := time.NewTimer(tickInterval)
	defer timer.Stop()
	for {
		select {
		case now := <-timer.C:
			t.snubCheck(now, timeout)
			timer.Reset(tickInterval)
		case <-t.closed.Done():
			return
		}
	}
}

func (t *Torrent) snubCheck(now time.Time, timeout time.Duration) {
	t.cl.lock()
	defer t.cl.unlock()
	t.iterPeers(func(p *Peer) {
		p.maybeSnub(now, timeout)
	})
}

// maybeSnub marks p as snubbed if it has outstanding requests but hasn't
// delivered a useful chunk in `timeout`. Cancels its outstanding requests
// so the global slots free up for other peers.
func (p *Peer) maybeSnub(now time.Time, timeout time.Duration) {
	if p.snubbed {
		return
	}
	pc, ok := p.legacyPeerImpl.(*PeerConn)
	if !ok {
		return
	}
	if pc.requestState.Requests.IsEmpty() {
		return
	}
	if p.lastUsefulChunkReceived.IsZero() {
		// Brand-new peer; don't snub before they've had a chance to respond.
		return
	}
	if now.Sub(p.lastUsefulChunkReceived) < timeout {
		return
	}
	p.snubbed = true
	pc.requestState.Requests.IterateSnapshot(func(r RequestIndex) bool {
		pc.cancel(r)
		return true
	})
	pc.peakRequests = 1
	pc.onNeedUpdateRequests("snubbed")
}

// clearSnub is called from the receive path on a successful chunk so a
// recovered peer can rebuild its pipeline.
func (p *Peer) clearSnub() {
	if !p.snubbed {
		return
	}
	p.snubbed = false
	p.peakRequests = 0
}
