package cache

import (
	"SimpleKV/sstable"
	"fmt"
	"sync"
)

type Cache struct {
	//index Replacer
	index     map[uint64]*sstable.IndexBlock
	table     Replacer
	indexLock sync.RWMutex
	tableLock sync.RWMutex
}

func NewCache(nblock, nindex int) *Cache {
	return &Cache{
		//index: NewLRUReplacer(nindex),
		index: make(map[uint64]*sstable.IndexBlock),
		table: NewLRUReplacer(100),
	}
}

func (cache Cache) AddTable(fid uint64, t *sstable.Table) {
	cache.tableLock.Lock()
	defer cache.tableLock.Unlock()
	cache.table.Put(fmt.Sprintf("%d", fid), t)
}

func (cache Cache) GetTable(fid uint64) *sstable.Table {
	cache.tableLock.RLock()
	defer cache.tableLock.RUnlock()
	t := cache.table.Get(fmt.Sprintf("%d", fid))
	if t == nil {
		return nil
	}
	return t.(*sstable.Table)
}

func (cache Cache) AddIndex(fid uint64, index *sstable.IndexBlock) {
	//cache.index.Put(fmt.Sprintf("%d", fid), b)
	cache.indexLock.Lock()
	defer cache.indexLock.Unlock()
	cache.index[fid] = index
}

func (cache Cache) GetIndex(fid uint64) *sstable.IndexBlock {
	cache.indexLock.RLock()
	cache.indexLock.RUnlock()
	index, ok := cache.index[fid]
	if ok {
		return index
	}
	return nil
	//index := cache.index.Get(fmt.Sprintf("%d", fid))
	//if index == nil {
	//	return nil
	//}
	//return index.(*sstable.IndexBlock)
}
