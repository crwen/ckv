package sstable

import (
	"SimpleKV/utils"
	"SimpleKV/utils/errs"
	"os"
)

// sst 的内存形式
type Table struct {
	ss *SSTable
	//lm     *levelManager
	fid    uint64
	opt    *utils.Options
	MinKey []byte
	MaxKey []byte
}

func newTable(opt *utils.Options, fid uint64) *Table {
	return &Table{
		fid: fid,
		opt: opt,
	}
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *Table
	blockPos int
	//bi       *blockIterator
	err error
}

func (t *Table) Serach(key []byte) (entry *utils.Entry, err error) {
	f, err := os.OpenFile(t.ss.GetName(), os.O_RDONLY, 644)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	index := t.ss.Indexs()
	filter := utils.Filter(index.Filter)
	if t.ss.HasBloomFilter() && !filter.MayContainKey(key) {
		return nil, errs.ErrKeyNotFound
	}
	idx := t.findGreater(index, key)
	if idx < 0 {
		return nil, nil
	}

	// search block
	block := &Block{}
	blockOffset := index.BlockOffsets[idx]
	offset := blockOffset.Offset
	size := blockOffset.Len

	buf := make([]byte, size)
	f.ReadAt(buf, int64(offset))

	block.Offset = int(offset)
	block.Data = buf

	offset = block.readEntryOffsets(buf)
	buf = buf[:offset]

	// TODO cache block

	for i, bo := range block.EntryOffsets {
		var k, v []byte
		if i == len(block.EntryOffsets)-1 {
			k, v = block.readEntry(buf[bo:], uint32(offset)-bo)
		} else {
			k, v = block.readEntry(buf[bo:], block.EntryOffsets[i+1]-bo)
		}

		if t.Compare(k, key) == 0 {
			return &utils.Entry{Key: k, Value: v}, nil
		}
	}
	return nil, nil

}

// findGreaterOrEqual
func (t *Table) findGreater(index *IndexBlock, key []byte) int {
	low, high := 0, len(index.BlockOffsets)-1

	for low < high {
		mid := (high-low)/2 + low
		if t.Compare(index.BlockOffsets[mid].Key, key) >= 0 {
			high = mid
		} else {
			low = mid + 1
		}

	}
	if t.Compare(index.BlockOffsets[low].Key, key) > 0 {
		return low - 1
	}

	return low
}

func (t *Table) Compare(key, key2 []byte) int {
	return t.opt.Comparable.Compare(key, key2)
}
