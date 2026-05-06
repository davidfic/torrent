package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	g "github.com/anacrolix/generics"

	"github.com/anacrolix/torrent/metainfo"
)

// File-based storage for torrents, that isn't yet bound to a particular torrent.
type fileClientImpl struct {
	opts NewFileClientOpts
	// Shared across all torrents from this client. Nil if disabled, or for
	// backends that don't use it (mmap).
	fdCache *fdCache
}

// All Torrent data stored in this baseDir. The info names of each torrent are used as directories.
func NewFile(baseDir string) ClientImplCloser {
	return NewFileWithCompletion(baseDir, pieceCompletionForDir(baseDir))
}

type NewFileClientOpts struct {
	// The base directory for all downloads.
	ClientBaseDir   string
	FilePathMaker   FilePathMaker
	TorrentDirMaker TorrentDirFilePathMaker
	// If part files are enabled, this will default to inferring completion from file names at
	// startup, and keep the rest in memory.
	PieceCompletion PieceCompletion
	UsePartFiles    g.Option[bool]
	Logger          *slog.Logger
	// FdCacheSize bounds the file descriptors the classic backend keeps
	// open. Zero is the default (~min(1024, RLIMIT_NOFILE/4)); negative
	// disables caching. No effect with the mmap backend.
	FdCacheSize int
}

// The specific part-files option or the default.
func (me NewFileClientOpts) partFiles() bool {
	return me.UsePartFiles.UnwrapOr(true)
}

// NewFileOpts creates a new ClientImplCloser that stores files using the OS native filesystem.
func NewFileOpts(opts NewFileClientOpts) ClientImplCloser {
	if opts.TorrentDirMaker == nil {
		opts.TorrentDirMaker = defaultPathMaker
	}
	if opts.FilePathMaker == nil {
		opts.FilePathMaker = func(opts FilePathMakerOpts) string {
			var parts []string
			if opts.Info.BestName() != metainfo.NoName {
				parts = append(parts, opts.Info.BestName())
			}
			return filepath.Join(append(parts, opts.File.BestPath()...)...)
		}
	}
	if opts.PieceCompletion == nil {
		if opts.partFiles() {
			opts.PieceCompletion = NewMapPieceCompletion()
		} else {
			opts.PieceCompletion = pieceCompletionForDir(opts.ClientBaseDir)
		}
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	c := &fileClientImpl{opts: opts}
	if opts.FdCacheSize >= 0 {
		size := opts.FdCacheSize
		if size == 0 {
			size = defaultFdCacheSize()
		}
		c.fdCache = newFdCache(size)
	}
	return c
}

func (me *fileClientImpl) Close() error {
	var cacheErr error
	if me.fdCache != nil {
		cacheErr = me.fdCache.close()
	}
	return errors.Join(cacheErr, me.opts.PieceCompletion.Close())
}

func (fs *fileClientImpl) OpenTorrent(
	ctx context.Context,
	info *metainfo.Info,
	infoHash metainfo.Hash,
) (_ TorrentImpl, err error) {
	dir := fs.opts.TorrentDirMaker(fs.opts.ClientBaseDir, info, infoHash)
	fs.opts.Logger.DebugContext(ctx, "opened file torrent storage", slog.String("dir", dir))
	metainfoFileInfos := info.UpvertedFiles()
	files := make([]fileExtra, len(metainfoFileInfos))
	for i, fileInfo := range metainfoFileInfos {
		filePath := filepath.Join(dir, fs.opts.FilePathMaker(FilePathMakerOpts{
			Info: info,
			File: &fileInfo,
		}))
		if !isSubFilepath(dir, filePath) {
			err = fmt.Errorf("file %v: path %q is not sub path of %q", i, filePath, dir)
			return
		}
		files[i].safeOsPath = filePath
		if metainfoFileInfos[i].Length == 0 {
			err = CreateNativeZeroLengthFile(filePath)
			if err != nil {
				err = fmt.Errorf("creating zero length file: %w", err)
				return
			}
		}
	}
	t := &fileTorrentImpl{
		info,
		files,
		metainfoFileInfos,
		info.FileSegmentsIndex(),
		infoHash,
		defaultFileIo(fs.fdCache),
		fs,
	}
	if t.partFiles() {
		err = t.setCompletionFromPartFiles()
		if err != nil {
			err = fmt.Errorf("setting completion from part files: %w", err)
			return
		}
	}
	return TorrentImpl{
		Piece: t.Piece,
		Close: t.Close,
	}, nil
}
