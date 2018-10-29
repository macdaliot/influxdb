// +build !windows,!plan9

package tsm1

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	totalanon     int64
	totalfile     int64
	unmappedBytes int64
)

func mmap(f *os.File, offset int64, length int) ([]byte, error) {
	sz := int64(length)
	pages := sz / 4096
	if sz%4096 > 0 {
		pages++
	}
	total := pages * 4096

	// anonymous mapping
	if f == nil {
		new := atomic.AddInt64(&totalanon, total)
		fmt.Printf("[TSM] ANON allocating %d bytes (%d KB). Total ANON: %d (%d KB)\n", total, total/1024, new, new/1024)
		return unix.Mmap(-1, 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	}

	mmap, err := unix.Mmap(int(f.Fd()), 0, length, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		new := atomic.AddInt64(&totalfile, total)
		fmt.Printf("[TSM] FILE allocating %d bytes (%d KB). FILE: %d (%d KB)\n", total, total/1024, new, new/1024)
		return nil, err
	}

	anon, file := atomic.LoadInt64(&totalanon), atomic.LoadInt64(&totalfile)
	total = (anon + file) - atomic.LoadInt64(&unmappedBytes)
	fmt.Printf("[TSM] TOTAL is %d bytes (%d KB)\n", total, total/1024)
	return mmap, nil
}

func munmap(b []byte) (err error) {
	sz := int64(len(b))
	pages := sz / 4096
	if sz%4096 > 0 {
		pages++
	}
	total := pages * 4096
	fmt.Printf("[TSM] unmapping %d bytes (%d KB)\n", total, total/1024)

	anon, file, unmappedBytes := atomic.LoadInt64(&totalanon), atomic.LoadInt64(&totalfile), atomic.LoadInt64(&unmappedBytes)
	total = (anon + file) - unmappedBytes
	fmt.Printf("[TSM] TOTAL is %d bytes (%d KB)\n", total, total/1024)
	return unix.Munmap(b)
}

func madviseDontNeed(b []byte) error {
	return madvise(b, syscall.MADV_DONTNEED)
}

// From: github.com/boltdb/bolt/bolt_unix.go
func madvise(b []byte, advice int) (err error) {
	return unix.Madvise(b, advice)
}
