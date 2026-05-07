//go:build unix

package storage

import (
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"golang.org/x/sys/unix"
)

// Returns io.EOF if there's no data after offset. That doesn't mean there isn't zeroes for a sparse
// hole. Note that lseek returns -1 on error.
func seekData(f *os.File, offset int64) (ret int64, err error) {
	ret, err = unix.Seek(int(f.Fd()), offset, unix.SEEK_DATA)
	// TODO: Handle filesystems that don't support sparse files.
	if err == unix.ENXIO {
		// File has no more data. Treat as short write like io.CopyN.
		err = io.EOF
	}
	return
}

var pageSize = unix.Getpagesize()

// Reserves 3/4 of NOFILE for other consumers (peers, sockets, the process
// itself). Capped at 1024 because past that point the LRU walk gets long
// before eviction quality justifies it.
func defaultFdCacheSize() int {
	var lim unix.Rlimit
	if unix.Getrlimit(unix.RLIMIT_NOFILE, &lim) != nil {
		return 256
	}
	n := int(lim.Cur) / 4
	if n > 1024 {
		n = 1024
	}
	if n < 16 {
		n = 16
	}
	return n
}

func msync(mm mmap.MMap, offset, nbytes int) error {
	getDown := offset % pageSize
	return unix.Msync(mm[offset-getDown:offset+nbytes], unix.MS_SYNC)
}
