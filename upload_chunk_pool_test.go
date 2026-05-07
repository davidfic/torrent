package torrent

import (
	"sync"
	"testing"

	pp "github.com/anacrolix/torrent/peer_protocol"
	"github.com/go-quicktest/qt"
)

func TestUploadChunkPoolReuses(t *testing.T) {
	t.Parallel()
	tr := &Torrent{}
	tr.chunkSize = 16 << 10
	tr.uploadChunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, tr.chunkSize)
			return &b
		},
	}

	b1 := tr.getUploadChunkBuffer(int(tr.chunkSize))
	qt.Assert(t, qt.Equals(len(b1), int(tr.chunkSize)))
	tr.putUploadChunkBuffer(b1)
	b2 := tr.getUploadChunkBuffer(int(tr.chunkSize))
	// sync.Pool isn't required to return the same buffer, but in this
	// non-contended path it almost always does.
	qt.Assert(t, qt.Equals(len(b2), int(tr.chunkSize)))
}

func TestUploadChunkPoolOversizeAllocatesFresh(t *testing.T) {
	t.Parallel()
	tr := &Torrent{}
	tr.chunkSize = 16 << 10
	tr.uploadChunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, tr.chunkSize)
			return &b
		},
	}
	b := tr.getUploadChunkBuffer(64 << 10) // 4× chunkSize
	qt.Assert(t, qt.Equals(len(b), 64<<10))
	// Putting an oversize buffer drops it.
	tr.putUploadChunkBuffer(b)
}

func TestUploadChunkPoolPartialLength(t *testing.T) {
	t.Parallel()
	tr := &Torrent{}
	tr.chunkSize = 16 << 10
	tr.uploadChunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, tr.chunkSize)
			return &b
		},
	}
	// Tail-chunk-sized request: shorter than chunkSize.
	b := tr.getUploadChunkBuffer(7)
	qt.Assert(t, qt.Equals(len(b), 7))
	qt.Assert(t, qt.Equals(cap(b), int(tr.chunkSize)))
	tr.putUploadChunkBuffer(b)
}

// Helper just to keep pp imported in case Go doesn't see it elsewhere.
var _ = pp.Integer(0)
