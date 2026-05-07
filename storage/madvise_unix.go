//go:build unix

package storage

import (
	"os"
	"strings"

	"github.com/edsrzf/mmap-go"
	"golang.org/x/sys/unix"
)

// madviseAfterMap applies the user-selected madvise hints to a freshly
// created mapping. Selection comes from TORRENT_STORAGE_MADV (a
// comma-separated list of {"random","sequential","willneed"} -- absent
// or empty means leave kernel default in place). Errors are ignored: the
// hint is best-effort.
func madviseAfterMap(mm mmap.MMap) {
	for _, advice := range madvisePostMapAdvice {
		_ = unix.Madvise(mm, advice)
	}
}

// madviseDontNeed pages out the byte range. Used by callers that want to
// reduce page-cache footprint after a one-shot read (e.g. piece hashing
// or upload of a large unlikely-to-repeat file). Page-aligned at the
// caller's offset; trailing partial page included if it covers data.
//
// Returns nil if the dontneed feature is disabled or the kernel rejects
// the advice -- it's a hint.
func madviseDontNeed(mm mmap.MMap, offset, length int) {
	if !madviseDontNeedEnabled || length == 0 || len(mm) == 0 {
		return
	}
	pageOff := offset % pageSize
	end := offset + length
	if end > len(mm) {
		end = len(mm)
	}
	_ = unix.Madvise(mm[offset-pageOff:end], unix.MADV_DONTNEED)
}

var (
	madvisePostMapAdvice  = parsePostMapAdvice(os.Getenv("TORRENT_STORAGE_MADV"))
	madviseDontNeedEnabled = os.Getenv("TORRENT_STORAGE_MADV_DONTNEED") == "1"
)

func parsePostMapAdvice(s string) []int {
	if s == "" {
		return nil
	}
	var out []int
	for _, p := range strings.Split(s, ",") {
		switch strings.TrimSpace(p) {
		case "random":
			out = append(out, unix.MADV_RANDOM)
		case "sequential":
			out = append(out, unix.MADV_SEQUENTIAL)
		case "willneed":
			out = append(out, unix.MADV_WILLNEED)
		case "normal", "":
			// no-op
		}
	}
	return out
}
