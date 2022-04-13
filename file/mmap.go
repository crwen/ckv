package file

import (
	"os"
)

// MmapFile represents an mmapd file and includes both the buffer to the data and the file descriptor.
type MmapFile struct {
	Data []byte
	Fd   *os.File
}
