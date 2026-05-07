package torrent

import (
	"sort"
	"testing"

	"github.com/go-quicktest/qt"
)

func TestLessByDownloadContribution(t *testing.T) {
	t.Parallel()
	a := &PeerConn{Peer: Peer{chokerRecentBytes: 100}}
	b := &PeerConn{Peer: Peer{chokerRecentBytes: 500}}
	c := &PeerConn{Peer: Peer{chokerRecentBytes: 250}}
	peers := []*PeerConn{a, b, c}
	sort.SliceStable(peers, lessByDownloadContribution(peers))
	qt.Assert(t, qt.IsTrue(peers[0] == b))
	qt.Assert(t, qt.IsTrue(peers[1] == c))
	qt.Assert(t, qt.IsTrue(peers[2] == a))
}

func TestChokerConfigDefaults(t *testing.T) {
	t.Parallel()
	c := &ChokerConfig{}
	qt.Assert(t, qt.Equals(c.numSlots(), 5))
	qt.Assert(t, qt.Equals(c.optimisticSlots(), 1))
	qt.Assert(t, qt.IsTrue(c.interval() > 0))
	qt.Assert(t, qt.IsTrue(c.optimisticInterval() > 0))

	explicit := &ChokerConfig{NumSlots: 10, OptimisticSlots: 2}
	qt.Assert(t, qt.Equals(explicit.numSlots(), 10))
	qt.Assert(t, qt.Equals(explicit.optimisticSlots(), 2))
}
