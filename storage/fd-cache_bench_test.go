package storage

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

const benchChunkSize = 1 << 14 // 16 KiB

// Per-chunk write hot path: open+write+close vs cached write.
func BenchmarkClassicWriteAtChunks(b *testing.B) {
	const fileSize = 64 << 20
	chunk := make([]byte, benchChunkSize)
	rand.Read(chunk)
	for _, name := range []string{"NoCache", "Cache"} {
		b.Run(name, func(b *testing.B) {
			path := makeBenchFile(b, fileSize)
			io := classicFileIo{}
			if name == "Cache" {
				io.cache = newFdCache(64)
				defer io.cache.close()
			}
			b.SetBytes(benchChunkSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w, err := io.openForWrite(path, fileSize)
				if err != nil {
					b.Fatal(err)
				}
				off := int64((i * benchChunkSize) % (fileSize - benchChunkSize))
				if _, err := w.WriteAt(chunk, off); err != nil {
					b.Fatal(err)
				}
				w.Close()
			}
		})
	}
}

// Upload-side read hot path.
func BenchmarkClassicReadAtChunks(b *testing.B) {
	const fileSize = 64 << 20
	for _, name := range []string{"NoCache", "Cache"} {
		b.Run(name, func(b *testing.B) {
			path := makeBenchFile(b, fileSize)
			io := classicFileIo{}
			if name == "Cache" {
				io.cache = newFdCache(64)
				defer io.cache.close()
			}
			buf := make([]byte, benchChunkSize)
			b.SetBytes(benchChunkSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := io.openForSharedRead(path)
				if err != nil {
					b.Fatal(err)
				}
				off := int64((i * benchChunkSize) % (fileSize - benchChunkSize))
				if _, err := r.ReadAt(buf, off); err != nil {
					b.Fatal(err)
				}
				r.Close()
			}
		})
	}
}

// Many goroutines hitting a small pool of files; exercises lock contention.
func BenchmarkClassicWriteAtParallel(b *testing.B) {
	const fileSize = 256 << 20
	const numFiles = 8
	chunk := make([]byte, benchChunkSize)
	rand.Read(chunk)
	for _, name := range []string{"NoCache", "Cache"} {
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			paths := make([]string, numFiles)
			for i := range paths {
				paths[i] = filepath.Join(dir, "f-"+string(rune('a'+i)))
				f, err := os.Create(paths[i])
				if err != nil {
					b.Fatal(err)
				}
				if err := f.Truncate(fileSize); err != nil {
					b.Fatal(err)
				}
				f.Close()
			}
			io := classicFileIo{}
			if name == "Cache" {
				io.cache = newFdCache(64)
				defer io.cache.close()
			}
			b.SetBytes(benchChunkSize)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var i int
				for pb.Next() {
					p := paths[i%numFiles]
					w, err := io.openForWrite(p, fileSize)
					if err != nil {
						b.Fatal(err)
					}
					off := int64((i * benchChunkSize) % (fileSize - benchChunkSize))
					if _, err := w.WriteAt(chunk, off); err != nil {
						b.Fatal(err)
					}
					w.Close()
					i++
				}
			})
		})
	}
}

func makeBenchFile(b *testing.B, size int64) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), "data.bin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		b.Fatal(err)
	}
	f.Close()
	return path
}
