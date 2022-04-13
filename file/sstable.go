package file

import (
	"sync"
)

// SSTable 文件的内存封装
type SSTable struct {
	lock *sync.RWMutex
	f    *MmapFile

	fid uint64
}
