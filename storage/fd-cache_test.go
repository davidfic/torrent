package storage

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-quicktest/qt"
)

func makeTestFiles(t *testing.T, dir string, n int) []string {
	t.Helper()
	paths := make([]string, n)
	for i := range paths {
		p := filepath.Join(dir, fmt.Sprintf("f-%04d", i))
		f, err := os.Create(p)
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.IsNil(f.Close()))
		paths[i] = p
	}
	return paths
}

func TestFdCacheHitReusesHandle(t *testing.T) {
	t.Parallel()
	c := newFdCache(4)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 1)

	e1, err := c.acquire(paths[0], false)
	qt.Assert(t, qt.IsNil(err))
	c.release(e1)
	e2, err := c.acquire(paths[0], false)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsTrue(e1 == e2))
	c.release(e2)

	hits, misses := c.hitRate()
	qt.Assert(t, qt.Equals(hits, int64(1)))
	qt.Assert(t, qt.Equals(misses, int64(1)))
}

func TestFdCacheLRUEvictsLeastRecent(t *testing.T) {
	t.Parallel()
	c := newFdCache(2)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 4)

	// Access pattern: 0, 1, 0 (refresh), 2 -> 1 evicted; 3 -> 0 evicted.
	for _, i := range []int{0, 1, 0} {
		e, _ := c.acquire(paths[i], false)
		c.release(e)
	}
	e2, _ := c.acquire(paths[2], false)
	c.release(e2)
	qt.Assert(t, qt.IsFalse(c.has(paths[1], false)))
	qt.Assert(t, qt.IsTrue(c.has(paths[0], false)))

	e3, _ := c.acquire(paths[3], false)
	c.release(e3)
	qt.Assert(t, qt.IsFalse(c.has(paths[0], false)))
}

func TestFdCachePinnedSurvivesEviction(t *testing.T) {
	t.Parallel()
	c := newFdCache(2)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 4)

	pinned, _ := c.acquire(paths[0], false)
	for _, p := range paths[1:] {
		e, _ := c.acquire(p, false)
		c.release(e)
	}
	// cap is advisory under pinning: paths[0] is still there.
	qt.Assert(t, qt.IsTrue(c.has(paths[0], false)))
	c.release(pinned)
}

func TestFdCacheInvalidate(t *testing.T) {
	t.Parallel()
	c := newFdCache(4)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 1)

	// Unpinned: invalidate closes immediately.
	e, _ := c.acquire(paths[0], false)
	c.release(e)
	c.invalidate(paths[0])
	qt.Assert(t, qt.IsFalse(c.has(paths[0], false)))

	// Pinned: invalidate detaches but the holder's fd survives until release.
	pinned, _ := c.acquire(paths[0], false)
	c.invalidate(paths[0])
	qt.Assert(t, qt.IsFalse(c.has(paths[0], false)))
	buf := make([]byte, 1)
	_, err := pinned.file.ReadAt(buf, 0) // fd is still valid
	qt.Assert(t, qt.IsTrue(err == nil || err.Error() == "EOF"))
	c.release(pinned)

	// Subsequent acquire opens fresh.
	e2, _ := c.acquire(paths[0], false)
	qt.Assert(t, qt.IsTrue(pinned != e2))
	c.release(e2)
}

func TestFdCacheReadWriteSeparate(t *testing.T) {
	t.Parallel()
	c := newFdCache(4)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 1)

	r, _ := c.acquire(paths[0], false)
	w, _ := c.acquire(paths[0], true)
	qt.Assert(t, qt.IsTrue(r != w))
	c.release(r)
	c.release(w)
}

func TestFdCacheClose(t *testing.T) {
	t.Parallel()
	c := newFdCache(4)
	paths := makeTestFiles(t, t.TempDir(), 1)

	e, _ := c.acquire(paths[0], false)
	c.release(e)
	qt.Assert(t, qt.IsNil(c.close()))

	_, err := c.acquire(paths[0], false)
	qt.Assert(t, qt.ErrorIs(err, fs.ErrClosed))
	qt.Assert(t, qt.IsNil(c.close())) // idempotent
}

func TestFdCacheConcurrentAcquireRelease(t *testing.T) {
	t.Parallel()
	c := newFdCache(4)
	defer c.close()
	paths := makeTestFiles(t, t.TempDir(), 8)

	const goroutines = 32
	const iters = 500
	var wg sync.WaitGroup
	var ops atomic.Int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				p := paths[(seed+i)%len(paths)]
				e, err := c.acquire(p, (seed^i)&1 == 0)
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				ops.Add(1)
				c.release(e)
			}
		}(g)
	}
	wg.Wait()
	qt.Assert(t, qt.Equals(ops.Load(), int64(goroutines*iters)))
}

// Exercises classicFileIo.rename: a rename must invalidate cached handles
// for both names, otherwise writes through a stale write-handle for the
// destination land on the now-detached old inode.
func TestClassicFileIoRenameInvalidatesCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	for _, p := range []string{src, dst} {
		f, err := os.Create(p)
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.IsNil(f.Truncate(16)))
		qt.Assert(t, qt.IsNil(f.Close()))
	}

	cache := newFdCache(4)
	defer cache.close()
	io := classicFileIo{cache: cache}

	// Cache a write handle for dst.
	w, err := io.openForWrite(dst, 16)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(w.Close()))
	qt.Assert(t, qt.IsTrue(cache.has(dst, true)))

	qt.Assert(t, qt.IsNil(io.rename(src, dst)))
	qt.Assert(t, qt.IsFalse(cache.has(dst, true)))
	qt.Assert(t, qt.IsFalse(cache.has(src, true)))

	// New write handle goes to the renamed inode.
	w, err = io.openForWrite(dst, 16)
	qt.Assert(t, qt.IsNil(err))
	_, err = w.WriteAt([]byte("ok"), 0)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(w.Close()))

	got, err := os.ReadFile(dst)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(got[:2]), "ok"))
}

// has is a test-only helper that inspects the cache without disturbing LRU.
func (c *fdCache) has(path string, write bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[fdKey{path, write}]
	return ok
}
