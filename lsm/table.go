package lsm

import (
	"SimpleKV/file"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}
