//go:build !wasm

package storage

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync/atomic"
	"weak"

	"github.com/anacrolix/sync"

	"github.com/anacrolix/missinggo/v2/panicif"
	"github.com/edsrzf/mmap-go"
)

// Lock uses of shared handles, instead of having a lifetime RLock. Because sync.RWMutex is not safe
// for recursive RLocks, you can't have both.
const lockHandleOperations = false

func init() {
	s, ok := os.LookupEnv("TORRENT_STORAGE_DEFAULT_FILE_IO")
	if !ok {
		defaultFileIo = func() fileIo {
			return newMmapFileIo()
		}
		return
	}
	switch s {
	case "mmap":
		defaultFileIo = func() fileIo {
			return newMmapFileIo()
		}
	case "classic":
		defaultFileIo = func() fileIo {
			return classicFileIo{}
		}
	default:
		panic(s)
	}
}

// mmapFileIo bounds the number of strongly-referenced file mappings under
// LRU. When a mapping is demoted off the strong list it's parked in a weak
// map so a quick re-acquire revives it without remapping. Once the
// fileMmap loses its last strong reference (cache + consumers all gone),
// the GC reclaims it via the runtime finalizer that munmaps and closes
// the fd.
type mmapFileIo struct {
	mu     sync.Mutex
	cap    int
	strong map[string]*list.Element // *lruEntry
	lru    *list.List
	soft   map[string]weak.Pointer[fileMmap]
	closed bool
}

type lruEntry struct {
	name string
	fm   *fileMmap
}

func newMmapFileIo() *mmapFileIo {
	return &mmapFileIo{
		cap:    defaultMmapStrongCap(),
		strong: make(map[string]*list.Element),
		lru:    list.New(),
		soft:   make(map[string]weak.Pointer[fileMmap]),
	}
}

func (me *mmapFileIo) Close() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	for _, elem := range me.strong {
		ent := elem.Value.(*lruEntry)
		ent.fm.dec() // drop store's ref; consumers, if any, still hold theirs
	}
	me.strong = nil
	me.lru = nil
	me.soft = nil
	me.closed = true
	return nil
}

func (me *mmapFileIo) closedErr() error {
	if me.closed {
		return fs.ErrClosed
	}
	return nil
}

// rename invalidates cache entries for both names. Existing consumers keep
// their mappings (their fm.refs > 0); future acquires re-open against the
// new path layout.
func (me *mmapFileIo) rename(from, to string) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	me.invalidateLocked(from)
	me.invalidateLocked(to)
	return os.Rename(from, to)
}

func (me *mmapFileIo) invalidateLocked(name string) {
	if elem, ok := me.strong[name]; ok {
		ent := elem.Value.(*lruEntry)
		me.lru.Remove(elem)
		delete(me.strong, name)
		ent.fm.dec()
	}
	delete(me.soft, name)
}

func (me *mmapFileIo) flush(name string, offset, nbytes int64) error {
	me.mu.Lock()
	v, ok := me.lookupLocked(name)
	me.mu.Unlock()
	if !ok || !v.writable {
		return nil
	}
	defer v.dec()
	return msync(v.m, int(offset), int(nbytes))
}

// lookupLocked returns a strongly-referenced fileMmap for name if cached.
// Bumps LRU on a strong hit and promotes from the soft map on a soft hit.
// The returned fm has had its refcount incremented; the caller must dec()
// it.
func (me *mmapFileIo) lookupLocked(name string) (*fileMmap, bool) {
	if elem, ok := me.strong[name]; ok {
		ent := elem.Value.(*lruEntry)
		me.lru.MoveToFront(elem)
		ent.fm.inc()
		return ent.fm, true
	}
	if wp, ok := me.soft[name]; ok {
		fm := wp.Value()
		if fm != nil && fm.tryAcquire() {
			me.initLocked()
			me.promoteLocked(name, fm)
			return fm, true
		}
		delete(me.soft, name)
	}
	return nil, false
}

func (me *mmapFileIo) promoteLocked(name string, fm *fileMmap) {
	ent := &lruEntry{name: name, fm: fm}
	elem := me.lru.PushFront(ent)
	me.strong[name] = elem
	delete(me.soft, name)
	me.evictLocked()
}

// evictLocked walks the LRU tail, demoting entries until under cap. Demote
// drops the store's ref; consumers (if any) keep the file alive. Once
// consumers release, the fm is finalized and its mmap is munmapped.
func (me *mmapFileIo) evictLocked() {
	for me.lru.Len() > me.cap {
		elem := me.lru.Back()
		if elem == nil {
			return
		}
		ent := elem.Value.(*lruEntry)
		me.lru.Remove(elem)
		delete(me.strong, ent.name)
		me.soft[ent.name] = weak.Make(ent.fm)
		ent.fm.dec()
	}
}

// Shared file access.
type fileMmap struct {
	mu       sync.RWMutex
	m        mmap.MMap
	f        *os.File
	refs     atomic.Int32
	writable bool
	closed   bool
}

func (me *fileMmap) dec() error {
	if me.refs.Add(-1) == 0 {
		return me.close()
	}
	return nil
}

func (me *fileMmap) close() (err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.closed {
		return
	}
	me.closed = true
	return errors.Join(me.m.Unmap(), me.f.Close())
}

func (me *fileMmap) inc() {
	panicif.LessThanOrEqual(me.refs.Add(1), 0)
}

// tryAcquire increments refs only if the fm hasn't been closed. Used when
// reviving from the soft map: a weak.Pointer can resolve to an fm whose
// refs hit 0 and is in the process of being closed; we must reject those.
func (me *fileMmap) tryAcquire() bool {
	for {
		cur := me.refs.Load()
		if cur == 0 {
			return false
		}
		if me.refs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (me *mmapFileIo) openForSharedRead(name string) (sharableReader, error) {
	return me.openReadOnly(name)
}

func (me *mmapFileIo) openForRead(name string) (fileReader, error) {
	sh, err := me.openReadOnly(name)
	if err != nil {
		return nil, err
	}
	return &mmapFileHandle{shared: sh}, nil
}

func (me *mmapFileIo) openReadOnly(name string) (*mmapSharedFileHandle, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return nil, err
	}
	if fm, ok := me.lookupLocked(name); ok {
		return newSharedHandle(fm), nil
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("mapping file: %w", err)
	}
	fm := me.insertLocked(name, mm, false, f)
	return newSharedHandle(fm), nil
}

func (me *mmapFileIo) openForWrite(name string, size int64) (fileWriter, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return nil, err
	}
	if fm, ok := me.lookupLocked(name); ok {
		if int64(len(fm.m)) == size && fm.writable {
			return newSharedHandle(fm), nil
		}
		// Wrong size or read-only mapping; replace.
		me.invalidateLocked(name)
		fm.dec() // drop the inc from lookupLocked
	}
	f, err := openFileExtra(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	closeFile := true
	defer func() {
		if closeFile {
			f.Close()
		}
	}()
	if err := f.Truncate(size); err != nil {
		return nil, fmt.Errorf("error truncating file: %w", err)
	}
	mm, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	if int64(len(mm)) != size {
		mm.Unmap()
		return nil, fmt.Errorf("new mmap has wrong size %v, expected %v", len(mm), size)
	}
	closeFile = false
	fm := me.insertLocked(name, mm, true, f)
	return newSharedHandle(fm), nil
}

// insertLocked installs a freshly-opened mapping into the cache and returns
// it with refcount = 2 (one for the cache, one for the caller).
func (me *mmapFileIo) insertLocked(name string, mm mmap.MMap, writable bool, f *os.File) *fileMmap {
	me.initLocked()
	fm := &fileMmap{m: mm, f: f, writable: writable}
	fm.refs.Store(1) // cache's ref
	ent := &lruEntry{name: name, fm: fm}
	elem := me.lru.PushFront(ent)
	me.strong[name] = elem
	delete(me.soft, name)
	me.evictLocked()
	fm.inc() // caller's ref
	return fm
}

// initLocked lazily initialises the cache so a zero-value mmapFileIo is
// usable without going through the constructor.
func (me *mmapFileIo) initLocked() {
	if me.cap == 0 {
		me.cap = defaultMmapStrongCap()
	}
	if me.strong == nil {
		me.strong = make(map[string]*list.Element)
	}
	if me.lru == nil {
		me.lru = list.New()
	}
	if me.soft == nil {
		me.soft = make(map[string]weak.Pointer[fileMmap])
	}
}

func newSharedHandle(fm *fileMmap) *mmapSharedFileHandle {
	if !lockHandleOperations {
		// We're holding the IO context lock so this can't fail.
		panicif.False(fm.mu.TryRLock())
	}
	return &mmapSharedFileHandle{
		f: fm,
		close: sync.OnceValue[error](func() error {
			if !lockHandleOperations {
				fm.mu.RUnlock()
			}
			return fm.dec()
		}),
	}
}

var _ fileIo = (*mmapFileIo)(nil)

type mmapSharedFileHandle struct {
	f     *fileMmap
	close func() error
}

func (me *mmapSharedFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	// The caller already has the buffer; mmap wouldn't help.
	return me.f.f.WriteAt(p, off)
}

func (me *mmapSharedFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	n = copy(p, me.f.m[off:])
	if n < len(p) {
		if off < 0 {
			err = fs.ErrInvalid
			return
		}
	}
	if off+int64(n) == int64(len(me.f.m)) {
		err = io.EOF
	}
	return
}

func (me *mmapSharedFileHandle) Close() error {
	return me.close()
}

type mmapFileHandle struct {
	shared *mmapSharedFileHandle
	pos    int64
}

func (me *mmapFileHandle) WriteTo(w io.Writer) (n int64, err error) {
	b := me.shared.f.m
	if me.pos >= int64(len(b)) {
		return
	}
	n1, err := w.Write(b[me.pos:])
	n = int64(n1)
	me.pos += n
	return
}

func (me *mmapFileHandle) writeToN(w io.Writer, n int64) (written int64, err error) {
	mu := &me.shared.f.mu
	if lockHandleOperations {
		mu.RLock()
	}
	b := me.shared.f.m
	panicif.Nil(b)
	if me.pos >= int64(len(b)) {
		return
	}
	b = b[me.pos:]
	b = b[:min(int64(len(b)), n)]
	i, err := w.Write(b)
	if lockHandleOperations {
		mu.RUnlock()
	}
	written = int64(i)
	me.pos += written
	return
}

func (me *mmapFileHandle) Close() error {
	return me.shared.Close()
}

func (me *mmapFileHandle) Read(p []byte) (n int, err error) {
	if me.pos > int64(len(me.shared.f.m)) {
		err = io.EOF
		return
	}
	n = copy(p, me.shared.f.m[me.pos:])
	me.pos += int64(n)
	if me.pos >= int64(len(me.shared.f.m)) {
		err = io.EOF
	}
	return
}

func (me *mmapFileHandle) seekDataOrEof(offset int64) (ret int64, err error) {
	mu := &me.shared.f.mu
	if lockHandleOperations {
		mu.RLock()
	}
	ret, err = seekData(me.shared.f.f, offset)
	if lockHandleOperations {
		mu.RUnlock()
	}
	switch err {
	case nil:
		me.pos = ret
	case io.EOF:
		err = nil
		ret = int64(len(me.shared.f.m))
		me.pos = ret
	default:
		ret = me.pos
	}
	return
}
