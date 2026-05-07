package storage

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"

	"github.com/anacrolix/missinggo/v2/panicif"
)

// fdCache holds open *os.File handles by path for reuse, replacing the
// open/write/close-per-chunk pattern in the classic file backend. Handles
// returned by acquire are shared and must only be used with positional I/O
// (ReadAt/WriteAt). The cache is bounded by an LRU; pinned entries (refs > 0)
// are skipped during eviction.
type fdCache struct {
	mu         sync.Mutex
	cap        int
	items      map[fdKey]*fdEntry
	head, tail *fdEntry
	closed     bool

	hits, misses atomic.Int64
}

type fdKey struct {
	path  string
	write bool
}

// fdEntry holds a single cached *os.File. It carries a back-pointer to its
// cache so that the user-visible handle (cachedHandle) can be one word and
// fit inline in an interface value.
type fdEntry struct {
	cache      *fdCache
	key        fdKey
	file       *os.File
	refs       int
	removed    bool
	prev, next *fdEntry
}

// newFdCache returns a cache of the given capacity. cap <= 0 returns nil
// (caching disabled).
func newFdCache(cap int) *fdCache {
	if cap <= 0 {
		return nil
	}
	return &fdCache{cap: cap, items: make(map[fdKey]*fdEntry, cap)}
}

// acquire pins an fdEntry for path, opening one if not cached. The caller
// must release it exactly once.
func (c *fdCache) acquire(path string, write bool) (*fdEntry, error) {
	key := fdKey{path, write}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, fs.ErrClosed
	}
	if e, ok := c.items[key]; ok {
		e.refs++
		c.bumpLocked(e)
		c.mu.Unlock()
		c.hits.Add(1)
		return e, nil
	}
	c.mu.Unlock()

	c.misses.Add(1)
	f, err := openForCache(path, write)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		f.Close()
		return nil, fs.ErrClosed
	}
	// Another goroutine may have inserted while we opened. Lose the race
	// gracefully: discard our handle, take theirs.
	if e, ok := c.items[key]; ok {
		e.refs++
		c.bumpLocked(e)
		c.mu.Unlock()
		f.Close()
		return e, nil
	}
	e := &fdEntry{cache: c, key: key, file: f, refs: 1}
	c.items[key] = e
	c.pushFrontLocked(e)
	c.evictLocked()
	c.mu.Unlock()
	return e, nil
}

// acquireExisting pins an entry only if it's already cached. Used by flush
// to fsync via a cached handle without forcing an open.
func (c *fdCache) acquireExisting(key fdKey) (*fdEntry, bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	e, ok := c.items[key]
	if ok {
		e.refs++
		c.bumpLocked(e)
	}
	c.mu.Unlock()
	if ok {
		c.hits.Add(1)
	}
	return e, ok
}

func openForCache(path string, write bool) (*os.File, error) {
	if write {
		// O_RDWR (not O_WRONLY) so the same handle can serve hashing reads.
		return openFileExtra(path, os.O_RDWR)
	}
	return os.Open(path)
}

// release returns a handle to the cache. If the entry was removed while
// pinned (eviction, invalidate, close), the last release closes the file.
func (c *fdCache) release(e *fdEntry) {
	c.mu.Lock()
	e.refs--
	panicif.LessThan(e.refs, 0)
	if e.removed && e.refs == 0 {
		c.mu.Unlock()
		e.file.Close()
		return
	}
	c.mu.Unlock()
}

// invalidate drops all cached entries for path. In-use handles keep working
// (they hold their own *os.File reference) and close the file on last
// release.
func (c *fdCache) invalidate(path string) {
	var toClose [2]*os.File
	n := 0
	c.mu.Lock()
	for _, write := range [...]bool{false, true} {
		e, ok := c.items[fdKey{path, write}]
		if !ok {
			continue
		}
		c.unlinkLocked(e)
		delete(c.items, e.key)
		e.removed = true
		if e.refs == 0 {
			toClose[n] = e.file
			n++
		}
	}
	c.mu.Unlock()
	for i := 0; i < n; i++ {
		toClose[i].Close()
	}
}

// close drains the cache. Pinned entries are marked removed and closed by
// their last releaser. Held throughout under the lock so it serializes with
// concurrent release.
func (c *fdCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var err error
	for _, e := range c.items {
		e.removed = true
		if e.refs == 0 {
			err = errors.Join(err, e.file.Close())
		}
	}
	c.items = nil
	c.head, c.tail = nil, nil
	return err
}

func (c *fdCache) hitRate() (int64, int64) {
	return c.hits.Load(), c.misses.Load()
}

// LRU helpers; all require c.mu.

func (c *fdCache) pushFrontLocked(e *fdEntry) {
	e.prev = nil
	e.next = c.head
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *fdCache) unlinkLocked(e *fdEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

func (c *fdCache) bumpLocked(e *fdEntry) {
	if c.head == e {
		return
	}
	c.unlinkLocked(e)
	c.pushFrontLocked(e)
}

func (c *fdCache) evictLocked() {
	for len(c.items) > c.cap {
		e := c.tail
		for e != nil && e.refs != 0 {
			e = e.prev
		}
		if e == nil {
			// Everything pinned. Cap is advisory under pinning.
			return
		}
		c.unlinkLocked(e)
		delete(c.items, e.key)
		e.removed = true
		e.file.Close()
	}
}
