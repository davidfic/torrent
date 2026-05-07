//go:build !wasm

package storage

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-quicktest/qt"
)

func makeMmapTestFile(t *testing.T, dir, name string, size int64) string {
	t.Helper()
	p := filepath.Join(dir, name)
	f, err := os.Create(p)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(f.Truncate(size)))
	qt.Assert(t, qt.IsNil(f.Close()))
	return p
}

func TestMmapStrongLRUEviction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	io := newMmapFileIo()
	io.cap = 2
	defer io.Close()

	a := makeMmapTestFile(t, dir, "a", 4096)
	b := makeMmapTestFile(t, dir, "b", 4096)
	c := makeMmapTestFile(t, dir, "c", 4096)

	for _, p := range []string{a, b, c} {
		r, err := io.openForSharedRead(p)
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.IsNil(r.Close()))
	}

	// a is now LRU and should have been demoted to soft.
	io.mu.Lock()
	_, strongA := io.strong[a]
	_, softA := io.soft[a]
	_, strongB := io.strong[b]
	_, strongC := io.strong[c]
	io.mu.Unlock()
	qt.Assert(t, qt.IsFalse(strongA))
	qt.Assert(t, qt.IsTrue(softA))
	qt.Assert(t, qt.IsTrue(strongB))
	qt.Assert(t, qt.IsTrue(strongC))
}

func TestMmapPinnedSurvivesEvictionPressure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	io := newMmapFileIo()
	io.cap = 1
	defer io.Close()

	a := makeMmapTestFile(t, dir, "a", 4096)
	b := makeMmapTestFile(t, dir, "b", 4096)

	// Pin a by holding its handle.
	pinned, err := io.openForSharedRead(a)
	qt.Assert(t, qt.IsNil(err))
	defer pinned.Close()

	// Open b — a is demoted (refs > 0 keeps it alive on the soft side).
	r, err := io.openForSharedRead(b)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(r.Close()))

	// Pinned handle still works.
	buf := make([]byte, 1)
	_, err = pinned.ReadAt(buf, 0)
	qt.Assert(t, qt.IsNil(err))
}

// After demotion, a quick re-open should revive the existing mapping
// rather than re-mmap. We can observe this by checking that the soft
// entry's pointer matches the strong entry's pointer post-promote.
func TestMmapSoftRevivePromotesExistingFm(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	io := newMmapFileIo()
	io.cap = 1
	defer io.Close()

	a := makeMmapTestFile(t, dir, "a", 4096)
	b := makeMmapTestFile(t, dir, "b", 4096)

	// Pin a so demotion doesn't immediately tear down.
	pinned, err := io.openForSharedRead(a)
	qt.Assert(t, qt.IsNil(err))
	defer pinned.Close()

	io.mu.Lock()
	originalFm := io.strong[a].Value.(*lruEntry).fm
	io.mu.Unlock()

	// Demote a.
	r, err := io.openForSharedRead(b)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(r.Close()))

	// Re-open a — should revive from soft.
	r2, err := io.openForSharedRead(a)
	qt.Assert(t, qt.IsNil(err))
	defer r2.Close()

	io.mu.Lock()
	revivedFm := io.strong[a].Value.(*lruEntry).fm
	io.mu.Unlock()
	qt.Assert(t, qt.IsTrue(originalFm == revivedFm))
}

// Open and release many distinct paths; expect address-space and FD use to
// stay bounded. We just check that the cache obeys cap; full memory
// auditing is left to the OS.
func TestMmapBoundedUnderManyPaths(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	io := newMmapFileIo()
	io.cap = 8
	defer io.Close()

	for i := 0; i < 64; i++ {
		p := makeMmapTestFile(t, dir, "f-"+string(rune('a'+i%26))+string(rune('a'+i/26)), 4096)
		r, err := io.openForSharedRead(p)
		qt.Assert(t, qt.IsNil(err))
		qt.Assert(t, qt.IsNil(r.Close()))
	}

	io.mu.Lock()
	strongLen := len(io.strong)
	io.mu.Unlock()
	if strongLen > io.cap {
		t.Fatalf("strong=%d > cap=%d", strongLen, io.cap)
	}
	// Force GC to clean up demoted entries we don't pin.
	runtime.GC()
}
