package torrent

import (
	"io"
	"os"
	"testing"

	"github.com/anacrolix/torrent/internal/testutil"
	qt "github.com/go-quicktest/qt"
)

// Forces the synchronous-write fallback by setting DiskWorkers < 0 and
// verifies an end-to-end download still completes.
func TestDiskPoolDisabledFallsBackToSync(t *testing.T) {
	seederDir, mi := testutil.GreetingTestTorrent()
	defer os.RemoveAll(seederDir)

	seederCfg := TestingConfig(t)
	seederCfg.Seed = true
	seederCfg.DataDir = seederDir
	seederCfg.DiskWorkers = -1
	seeder, err := NewClient(seederCfg)
	qt.Assert(t, qt.IsNil(err))
	defer seeder.Close()
	seederT, _, _ := seeder.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	seederT.VerifyData()

	leecherCfg := TestingConfig(t)
	leecherCfg.DiskWorkers = -1
	leecher, err := NewClient(leecherCfg)
	qt.Assert(t, qt.IsNil(err))
	defer leecher.Close()
	qt.Assert(t, qt.IsNil(leecher.diskPool))

	leecherT, _, _ := leecher.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	qt.Assert(t, qt.IsNil(leecherT.writeCompletions))

	leecherT.AddClientPeer(seeder)

	r := leecherT.NewReader()
	defer r.Close()
	r.SetContext(t.Context())
	got, err := io.ReadAll(r)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(got), testutil.GreetingFileContents))
}

// Verifies that the store buffer serves hash-time chunk reads from memory
// when the chunks were just received, avoiding storage reads on the
// freshly-downloaded piece.
func TestStoreBufferServesFreshHashReads(t *testing.T) {
	seederDir, mi := testutil.GreetingTestTorrent()
	defer os.RemoveAll(seederDir)

	seederCfg := TestingConfig(t)
	seederCfg.Seed = true
	seederCfg.DataDir = seederDir
	seeder, err := NewClient(seederCfg)
	qt.Assert(t, qt.IsNil(err))
	defer seeder.Close()
	seederT, _, _ := seeder.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	seederT.VerifyData()

	leecherCfg := TestingConfig(t)
	// Default MaxStoreBufferBytes (64 MiB) covers the whole torrent.
	leecher, err := NewClient(leecherCfg)
	qt.Assert(t, qt.IsNil(err))
	defer leecher.Close()
	leecherT, _, _ := leecher.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	qt.Assert(t, qt.IsNotNil(leecherT.storeBuf))
	leecherT.AddClientPeer(seeder)

	r := leecherT.NewReader()
	defer r.Close()
	r.SetContext(t.Context())
	got, err := io.ReadAll(r)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(string(got), testutil.GreetingFileContents))

	hits := leecherT.storeBuf.hitsHash.Load()
	misses := leecherT.storeBuf.misses.Load()
	if hits == 0 {
		t.Fatalf("expected hash-path cache hits, got %d hits, %d misses", hits, misses)
	}
}
