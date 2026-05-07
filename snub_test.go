package torrent

import (
	"testing"
	"time"

	"github.com/go-quicktest/qt"
)

func TestMaybeSnubNoOpWithoutOutstandingRequests(t *testing.T) {
	t.Parallel()
	pc := &PeerConn{}
	pc.lastUsefulChunkReceived = time.Now().Add(-time.Hour)
	pc.maybeSnub(time.Now(), 30*time.Second)
	qt.Assert(t, qt.IsFalse(pc.snubbed))
}

func TestClearSnub(t *testing.T) {
	t.Parallel()
	pc := &PeerConn{}
	pc.snubbed = true
	pc.peakRequests = 1
	pc.clearSnub()
	qt.Assert(t, qt.IsFalse(pc.snubbed))
	qt.Assert(t, qt.Equals(int(pc.peakRequests), 0))
}

func TestClearSnubNoOpIfNotSnubbed(t *testing.T) {
	t.Parallel()
	pc := &PeerConn{}
	pc.peakRequests = 5
	pc.clearSnub()
	qt.Assert(t, qt.Equals(int(pc.peakRequests), 5))
}

func TestNominalMaxRequestsWhenSnubbed(t *testing.T) {
	t.Parallel()
	pc := &PeerConn{}
	pc.snubbed = true
	qt.Assert(t, qt.Equals(int(pc.nominalMaxRequests()), 1))
}
