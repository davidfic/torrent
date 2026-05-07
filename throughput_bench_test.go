package torrent

import (
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anacrolix/log"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

// BenchmarkLoopbackDownload measures end-to-end download throughput on a
// single seeder + single leecher in-process. Each iteration creates fresh
// clients, downloads sizeBytes of pseudo-random data, and tears them down.
// b.SetBytes(sizeBytes) lets `go test -bench` report MB/s.
//
// Tunable via BENCH_TORRENT_BYTES (default 32 MiB).
func BenchmarkLoopbackDownload(b *testing.B) {
	sizeBytes := int64(32 << 20)
	if v := os.Getenv("BENCH_TORRENT_BYTES"); v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			sizeBytes = n
		}
	}

	dataDir, mi := makeRandomTorrent(b, sizeBytes)
	defer os.RemoveAll(dataDir)

	b.SetBytes(sizeBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runLoopbackDownload(b, dataDir, mi)
	}
}

func benchClientConfig(b *testing.B) *ClientConfig {
	// Build off TestingConfig so the bench uses the same plumbing the
	// existing transfer tests do, then loosen the throughput-relevant
	// knobs that make TestingConfig unsuitable for benchmarking.
	cfg := TestingConfig(b)
	cfg.MaxAllocPeerRequestDataPerConn = 1 << 20
	cfg.KeepAliveTimeout = 30 * time.Second
	cfg.DropMutuallyCompletePeers = false
	cfg.Logger = log.Default.WithFilterLevel(log.Critical)
	return cfg
}

func makeRandomTorrent(b *testing.B, sizeBytes int64) (string, *metainfo.MetaInfo) {
	b.Helper()
	dir, err := os.MkdirTemp("", "bench-torrent-*")
	if err != nil {
		b.Fatal(err)
	}
	dataPath := filepath.Join(dir, "data")
	f, err := os.Create(dataPath)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := io.CopyN(f, cryptorand.Reader, sizeBytes); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}

	info := metainfo.Info{
		Name:        "data",
		PieceLength: 1 << 20,
		Length:      sizeBytes,
	}
	if err := info.GeneratePieces(func(fi metainfo.FileInfo) (io.ReadCloser, error) {
		return os.Open(dataPath)
	}); err != nil {
		b.Fatal(err)
	}
	mi := &metainfo.MetaInfo{}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		b.Fatal(err)
	}
	return dir, mi
}

func runLoopbackDownload(b *testing.B, seederDir string, mi *metainfo.MetaInfo) {
	b.Helper()

	seederCfg := benchClientConfig(b)
	seederCfg.Seed = true
	seederCfg.DataDir = seederDir
	configureBenchClient(seederCfg)
	seeder, err := NewClient(seederCfg)
	if err != nil {
		b.Fatal(err)
	}
	defer seeder.Close()
	seederT, _, err := seeder.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	if err != nil {
		b.Fatal(err)
	}
	seederT.VerifyData()
	<-seederT.Complete().On()

	leecherCfg := benchClientConfig(b)
	configureBenchClient(leecherCfg)
	leecher, err := NewClient(leecherCfg)
	if err != nil {
		b.Fatal(err)
	}
	defer leecher.Close()
	leecherT, _, err := leecher.AddTorrentSpec(TorrentSpecFromMetaInfo(mi))
	if err != nil {
		b.Fatal(err)
	}
	leecherT.AddClientPeer(seeder)
	r := leecherT.NewReader()
	defer r.Close()
	r.SetReadahead(1 << 30)
	r.SetResponsive()
	if _, err := io.Copy(io.Discard, r); err != nil {
		b.Fatal(err)
	}
}
