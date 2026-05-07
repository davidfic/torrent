package torrent

import (
	"sync"
	"testing"

	pp "github.com/anacrolix/torrent/peer_protocol"
	"github.com/go-quicktest/qt"
)

// newStubTorrent builds the minimum a storeBuffer needs: a chunkPool
// of fixed-size slices and a chunkSize.
func newStubTorrent(chunkSize int) *Torrent {
	tr := &Torrent{}
	tr.chunkSize = pp.Integer(chunkSize)
	tr.chunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, chunkSize)
			return &b
		},
	}
	return tr
}

func TestStoreBufferEvictsPersistedLRU(t *testing.T) {
	t.Parallel()
	tr := newStubTorrent(4)
	sb := newStoreBuffer(tr, 8) // 2 chunks worth
	defer sb.close()

	// Insert 3 persisted chunks; oldest should be evicted.
	for i := 0; i < 3; i++ {
		key := storeBufferKey{piece: 0, chunk: chunkIndexType(i)}
		sb.put(key, tr.getChunkBuffer()[:4])
		sb.markPersisted(key)
	}
	qt.Assert(t, qt.IsFalse(sb.contains(storeBufferKey{0, 0})))
	qt.Assert(t, qt.IsTrue(sb.contains(storeBufferKey{0, 1})))
	qt.Assert(t, qt.IsTrue(sb.contains(storeBufferKey{0, 2})))
}

func TestStoreBufferUnpersistedSurvivesEviction(t *testing.T) {
	t.Parallel()
	tr := newStubTorrent(4)
	sb := newStoreBuffer(tr, 8)
	defer sb.close()

	// Three unpersisted entries; cap is advisory under pinning.
	for i := 0; i < 3; i++ {
		key := storeBufferKey{piece: 0, chunk: chunkIndexType(i)}
		sb.put(key, tr.getChunkBuffer()[:4])
	}
	for i := 0; i < 3; i++ {
		qt.Assert(t, qt.IsTrue(sb.contains(storeBufferKey{0, chunkIndexType(i)})))
	}
}

func TestStoreBufferGetReleaseHit(t *testing.T) {
	t.Parallel()
	tr := newStubTorrent(4)
	sb := newStoreBuffer(tr, 16)
	defer sb.close()

	key := storeBufferKey{0, 0}
	buf := tr.getChunkBuffer()
	buf[0], buf[1] = 'h', 'i'
	sb.put(key, buf[:2])
	sb.markPersisted(key)

	data, ref, ok := sb.get(key, false)
	qt.Assert(t, qt.IsTrue(ok))
	qt.Assert(t, qt.Equals(string(data), "hi"))
	ref.release()
}

// A pinned entry must keep working when replaced. Simulates a piece reset
// re-receiving a chunk while a hasher or upload reader is mid-Get.
func TestStoreBufferReplaceWhilePinned(t *testing.T) {
	t.Parallel()
	tr := newStubTorrent(4)
	sb := newStoreBuffer(tr, 16)
	defer sb.close()

	key := storeBufferKey{0, 0}
	first := tr.getChunkBuffer()
	first[0] = 1
	sb.put(key, first[:1])
	sb.markPersisted(key)

	data, ref, ok := sb.get(key, false)
	qt.Assert(t, qt.IsTrue(ok))
	qt.Assert(t, qt.Equals(data[0], byte(1)))

	second := tr.getChunkBuffer()
	second[0] = 2
	sb.put(key, second[:1])
	// Old reader still sees its data.
	qt.Assert(t, qt.Equals(data[0], byte(1)))
	ref.release()

	data2, ref2, ok := sb.get(key, false)
	qt.Assert(t, qt.IsTrue(ok))
	qt.Assert(t, qt.Equals(data2[0], byte(2)))
	ref2.release()
}

func TestStoreBufferRemoveAndFree(t *testing.T) {
	t.Parallel()
	tr := newStubTorrent(4)
	sb := newStoreBuffer(tr, 16)
	defer sb.close()

	key := storeBufferKey{0, 0}
	sb.put(key, tr.getChunkBuffer()[:4])
	qt.Assert(t, qt.IsTrue(sb.contains(key)))
	sb.removeAndFree(key)
	qt.Assert(t, qt.IsFalse(sb.contains(key)))
}

// contains is a test-only inspection helper.
func (sb *storeBuffer) contains(key storeBufferKey) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	_, ok := sb.items[key]
	return ok
}
