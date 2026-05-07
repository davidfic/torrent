//go:build wasm || windows

package storage

import (
	"io"
	"os"
)

func seekData(f *os.File, offset int64) (ret int64, err error) {
	return f.Seek(offset, io.SeekStart)
}

// Platforms without RLIMIT_NOFILE: pick something reasonable.
func defaultFdCacheSize() int {
	return 256
}
