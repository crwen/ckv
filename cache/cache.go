package cache

import (
	"SimpleKV/sstable"
)

type Cache struct {
	index Replacer
	block Replacer
}

func NewCache(nblock, nindex int) *Cache {
	return &Cache{
		index: NewLRUReplacer(nindex),
		block: NewLRUReplacer(nblock),
	}
}

func (cache Cache) AddBlock(key []byte, b *sstable.Block) {
	cache.block.Put(string(key), b)
}

func (cache Cache) GetBlock(key []byte) *sstable.Block {
	return cache.block.Get(string(key)).(*sstable.Block)
}

func (cache Cache) AddIndex(key []byte, b *sstable.IndexBlock) {
	cache.index.Put(string(key), b)
}

func (cache Cache) PutIndex(key []byte) *sstable.IndexBlock {
	return cache.index.Get(string(key)).(*sstable.IndexBlock)
}
