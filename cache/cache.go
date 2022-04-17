package cache

import (
	"SimpleKV/sstable"
	"fmt"
)

type Cache struct {
	//index Replacer
	index map[uint64]*sstable.IndexBlock
	table Replacer
}

func NewCache(nblock, nindex int) *Cache {
	return &Cache{
		//index: NewLRUReplacer(nindex),
		index: make(map[uint64]*sstable.IndexBlock),
		table: NewLRUReplacer(100),
	}
}

func (cache Cache) AddTable(fid uint64, t *sstable.Table) {
	cache.table.Put(fmt.Sprintf("%d", fid), t)
}

func (cache Cache) GetTable(fid uint64) *sstable.Table {
	t := cache.table.Get(fmt.Sprintf("%d", fid))
	if t == nil {
		return nil
	}
	return t.(*sstable.Table)
}

func (cache Cache) AddIndex(fid uint64, index *sstable.IndexBlock) {
	//cache.index.Put(fmt.Sprintf("%d", fid), b)
	cache.index[fid] = index
}

func (cache Cache) GetIndex(fid uint64) *sstable.IndexBlock {
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
