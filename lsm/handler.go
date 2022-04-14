package lsm

import (
	"sync"
)

type levelHandler struct {
	sync.RWMutex
	levelNum       int      // level
	tables         []*table // all tables for this level
	totalSize      int64    // the size of tables
	totalStaleSize int64
	lm             *levelManager
}

func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}
