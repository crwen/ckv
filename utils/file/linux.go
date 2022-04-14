package file

import (
	"golang.org/x/sys/unix"
	"os"
	"unsafe"
)

// mmap uses the mmap system call to memory-map a file. If writable is true,
// fd: the file descriptor to be mapped
// writable: the memory to be mapped is writable
// size: the size to be mapped
// return
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE // can be write
	}
	// map with sharing.
	// the data write to the memory will be copy to file, and will be seen by others
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// munmap unmaps a previously mapped slice.
func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// msync writes any modified data to persistent storage.
func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
