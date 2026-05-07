//go:build !unix

package storage

import "github.com/edsrzf/mmap-go"

func madviseAfterMap(mm mmap.MMap)                {}
func madviseDontNeed(mm mmap.MMap, offset, n int) {}
