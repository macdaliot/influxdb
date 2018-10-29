// +build darwin dragonfly freebsd linux nacl netbsd openbsd

// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mmap provides a way to memory-map a file.
package mmap

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
)

var (
	totalfile int64
)

// Map memory-maps a file.
func Map(path string, sz int64) ([]byte, error) {
	pages := sz / 4096
	if sz%4096 > 0 {
		pages++
	}
	total := pages * 4096

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	} else if fi.Size() == 0 {
		return nil, nil
	}

	// Use file size if map size is not passed in.
	if sz == 0 {
		sz = fi.Size()
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(sz), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	new := atomic.AddInt64(&totalfile, int64(sz))
	fmt.Printf("[PKG] allocating %d bytes (%d KB) %s\n", total, total*1024, path)
	fmt.Printf("[PKG] TOTAL is %d bytes (%d KB)\n", new, new*1024)
	return data, nil
}

// Unmap closes the memory-map.
func Unmap(data []byte) error {
	if data == nil {
		fmt.Println("DATA NIL!@!@£$!@$!@£!@")
		return nil
	}

	pages := int64(len(data)) / 4096
	if len(data)%4096 > 0 {
		pages++
	}
	total := pages * 4096

	new := atomic.AddInt64(&totalfile, -total)
	fmt.Printf("[PKG] unmapping %d bytes (%d KB)\n", total, total*1024)
	fmt.Printf("[PKG] TOTAL is %d bytes (%d KB)\n", new, new*1024)
	return syscall.Munmap(data)
}
