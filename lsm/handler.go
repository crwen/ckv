package lsm

import (
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/utils/errs"
	"sync"
)

type levelHandler struct {
	sync.RWMutex
	levelNum       int              // level
	tables         []*sstable.Table // all tables for this level
	totalSize      int64            // the size of tables
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

func (lh *levelHandler) add(t *sstable.Table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	return lh.searchL0SST(key)
}

func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	for _, t := range lh.tables {
		if t != nil && comparator.Compare(t.MinKey, key) <= 0 &&
			comparator.Compare(t.MaxKey, key) >= 0 {
			if entry, err := t.Serach(key); err == nil && entry != nil {
				return entry, nil
			}
		}
	}
	return nil, errs.ErrKeyNotFound
}
