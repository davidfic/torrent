package storage

import (
	"io"
	"os"
)

// classicFileIo opens files on demand. With a non-nil cache, file
// descriptors are pooled so the receive and upload hot paths skip the
// open/close syscall pair on every chunk.
type classicFileIo struct {
	cache *fdCache
}

// Close is a no-op. The cache outlives the per-torrent fileIo and is closed
// by fileClientImpl.
func (me classicFileIo) Close() error {
	return nil
}

func (me classicFileIo) rename(from, to string) error {
	if me.cache != nil {
		// Drop both names: the destination's open handle would otherwise
		// survive the rename pointing at the now-unlinked inode and silently
		// swallow writes.
		me.cache.invalidate(to)
		me.cache.invalidate(from)
	}
	return os.Rename(from, to)
}

func (me classicFileIo) flush(name string, offset, nbytes int64) error {
	if me.cache != nil {
		// Reuse the cached writable handle if we have one. Falls through to
		// a fresh open if not (read-only file, or never written via this IO).
		if e, ok := me.cache.acquireExisting(fdKey{name, true}); ok {
			err := e.file.Sync()
			e.cache.release(e)
			return err
		}
	}
	return fsync(name)
}

func (me classicFileIo) openForSharedRead(name string) (sharableReader, error) {
	if me.cache != nil {
		e, err := me.cache.acquire(name, false)
		if err != nil {
			return nil, err
		}
		return cachedHandle{e: e}, nil
	}
	return os.Open(name)
}

// openForRead returns a non-shared sequential reader. The fileReader
// contract uses the file's seek pointer (Read, seekDataOrEof, writeToN)
// which can't be safely shared, so this path doesn't use the cache.
func (me classicFileIo) openForRead(name string) (fileReader, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return classicFileReader{f}, nil
}

func (me classicFileIo) openForWrite(p string, size int64) (fileWriter, error) {
	if me.cache != nil {
		e, err := me.cache.acquire(p, true)
		if err != nil {
			return nil, err
		}
		return cachedHandle{e: e}, nil
	}
	return openFileExtra(p, os.O_WRONLY)
}

type classicFileReader struct {
	*os.File
}

func (c classicFileReader) writeToN(w io.Writer, n int64) (written int64, err error) {
	lw := limitWriter{
		rem: n,
		w:   w,
	}
	return c.File.WriteTo(&lw)
}

func (c classicFileReader) seekDataOrEof(offset int64) (ret int64, err error) {
	ret, err = seekData(c.File, offset)
	if err == io.EOF {
		ret, err = c.File.Seek(0, io.SeekEnd)
	}
	return
}

// cachedHandle wraps an *fdEntry. One word so it fits inline in an interface
// value (no heap alloc per acquire). Each acquire returns a fresh handle;
// Close releases its own ref.
type cachedHandle struct {
	e *fdEntry
}

func (h cachedHandle) ReadAt(p []byte, off int64) (int, error) {
	return h.e.file.ReadAt(p, off)
}

func (h cachedHandle) WriteAt(p []byte, off int64) (int, error) {
	return h.e.file.WriteAt(p, off)
}

func (h cachedHandle) Close() error {
	h.e.cache.release(h.e)
	return nil
}
