package lsm

import (
	"SimpleKV/file"
)

// sst 的内存形式
type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}
