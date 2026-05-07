package torrent

import (
	"sync"
	"sync/atomic"

	"github.com/anacrolix/missinggo/v2/panicif"
)

// storeBuffer keeps recently-received chunks in memory keyed by (piece,
// chunk). Reads -- piece hashing on completion, peer upload requests --
// hit it before falling back to storage. libtorrent's store_buffer.
//
// Lifetime: an entry is inserted before the disk write is submitted and
// marked persisted when the write completes. Eviction skips unpersisted
// and pinned entries. Buffers are taken from and returned to the
// torrent's chunkPool on eviction.
type storeBuffer struct {
	t *Torrent

	mu         sync.Mutex
	items      map[storeBufferKey]*storeBufferEntry
	head, tail *storeBufferEntry
	curBytes   int64
	maxBytes   int64

	hitsUpload, hitsHash, misses atomic.Int64
}

type storeBufferKey struct {
	piece pieceIndex
	chunk chunkIndexType
}

type storeBufferEntry struct {
	key       storeBufferKey
	data      []byte
	persisted bool
	removed   bool // unlinked from map; last release frees the buffer
	refs      int
	prev      *storeBufferEntry
	next      *storeBufferEntry
}

// storeBufferRef pins a get'd entry. Caller must call release.
type storeBufferRef struct {
	sb *storeBuffer
	e  *storeBufferEntry
}

func newStoreBuffer(t *Torrent, maxBytes int64) *storeBuffer {
	if maxBytes <= 0 {
		return nil
	}
	return &storeBuffer{
		t:        t,
		items:    make(map[storeBufferKey]*storeBufferEntry),
		maxBytes: maxBytes,
	}
}

// put stores data under key. If an entry already exists for the key, it's
// replaced -- the old entry is unlinked, freed if unpinned, and marked
// removed otherwise (existing readers' release calls will free it).
func (sb *storeBuffer) put(key storeBufferKey, data []byte) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if old, ok := sb.items[key]; ok {
		sb.unlinkLocked(old)
		delete(sb.items, key)
		sb.curBytes -= int64(len(old.data))
		old.removed = true
		if old.refs == 0 {
			sb.t.putChunkBuffer(old.data)
		}
	}
	e := &storeBufferEntry{key: key, data: data}
	sb.items[key] = e
	sb.pushFrontLocked(e)
	sb.curBytes += int64(len(data))
	sb.evictLocked()
}

// markPersisted flags an entry as eligible for eviction. No-op if the
// entry is already gone.
func (sb *storeBuffer) markPersisted(key storeBufferKey) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	e, ok := sb.items[key]
	if !ok {
		return
	}
	e.persisted = true
	sb.evictLocked()
}

// get pins the entry for key. The caller must call ref.release() exactly
// once when finished. Returns a zero ref and false on miss.
func (sb *storeBuffer) get(key storeBufferKey, fromHasher bool) ([]byte, storeBufferRef, bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	e, ok := sb.items[key]
	if !ok {
		sb.misses.Add(1)
		return nil, storeBufferRef{}, false
	}
	e.refs++
	sb.bumpLocked(e)
	if fromHasher {
		sb.hitsHash.Add(1)
	} else {
		sb.hitsUpload.Add(1)
	}
	return e.data, storeBufferRef{sb: sb, e: e}, true
}

func (ref storeBufferRef) release() {
	sb := ref.sb
	sb.mu.Lock()
	e := ref.e
	e.refs--
	panicif.LessThan(e.refs, 0)
	if e.removed && e.refs == 0 {
		sb.mu.Unlock()
		sb.t.putChunkBuffer(e.data)
		return
	}
	sb.evictLocked()
	sb.mu.Unlock()
}

// removeAndFree drops the entry and returns its buffer to the chunk pool.
// Used on write failure to roll back a put. The caller must ensure no
// outstanding reads (refs == 0); receive-path callers are guaranteed this
// because reads only target chunks of pieces that have completed writing.
func (sb *storeBuffer) removeAndFree(key storeBufferKey) {
	sb.mu.Lock()
	e, ok := sb.items[key]
	if !ok {
		sb.mu.Unlock()
		return
	}
	panicif.NotZero(e.refs)
	sb.unlinkLocked(e)
	delete(sb.items, key)
	sb.curBytes -= int64(len(e.data))
	sb.mu.Unlock()
	sb.t.putChunkBuffer(e.data)
}

// close releases all unpinned buffers to the chunk pool and tears down the
// cache. Pinned entries leak their buffer (a reader holds it until release;
// at that point the entry is unreachable and its slice is GCd).
func (sb *storeBuffer) close() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	for _, e := range sb.items {
		if e.refs == 0 {
			sb.t.putChunkBuffer(e.data)
		}
	}
	sb.items = nil
	sb.head, sb.tail = nil, nil
	sb.curBytes = 0
}

// evictLocked walks LRU back to front, evicting persisted+unpinned
// entries until under cap. If everything is pinned or unpersisted we
// return and accept the temporary overshoot.
func (sb *storeBuffer) evictLocked() {
	for sb.curBytes > sb.maxBytes {
		var victim *storeBufferEntry
		for e := sb.tail; e != nil; e = e.prev {
			if e.persisted && e.refs == 0 {
				victim = e
				break
			}
		}
		if victim == nil {
			return
		}
		sb.unlinkLocked(victim)
		delete(sb.items, victim.key)
		sb.curBytes -= int64(len(victim.data))
		sb.t.putChunkBuffer(victim.data)
	}
}

// LRU primitives. All require sb.mu.

func (sb *storeBuffer) pushFrontLocked(e *storeBufferEntry) {
	e.prev = nil
	e.next = sb.head
	if sb.head != nil {
		sb.head.prev = e
	}
	sb.head = e
	if sb.tail == nil {
		sb.tail = e
	}
}

func (sb *storeBuffer) unlinkLocked(e *storeBufferEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		sb.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		sb.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (sb *storeBuffer) bumpLocked(e *storeBufferEntry) {
	if sb.head == e {
		return
	}
	sb.unlinkLocked(e)
	sb.pushFrontLocked(e)
}

// storeBufferReadChunk attempts to satisfy a peer request from the cache.
// Returns a freshly allocated copy and true on hit, nil and false on miss
// (including for non-chunk-aligned or wrong-sized requests).
func (t *Torrent) storeBufferReadChunk(r Request) ([]byte, bool) {
	if t.storeBuf == nil {
		return nil, false
	}
	chunkSize := int64(t.chunkSize)
	if int64(r.Begin)%chunkSize != 0 {
		return nil, false
	}
	chunk := chunkIndexType(int64(r.Begin) / chunkSize)
	key := storeBufferKey{pieceIndex(r.Index), chunk}
	data, ref, ok := t.storeBuf.get(key, false)
	if !ok {
		return nil, false
	}
	if len(data) != int(r.Length) {
		ref.release()
		return nil, false
	}
	out := make([]byte, len(data))
	copy(out, data)
	ref.release()
	return out, true
}
