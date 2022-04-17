package sstable

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"SimpleKV/utils/errs"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
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

func OpenTable(opt *utils.Options, fid uint64) *Table {
	fileName := file.FileNameSSTable(opt.WorkDir, fid)
	t := &Table{fid: fid, opt: opt}
	t.ss = OpenSStable(&file.Options{
		FID:      fid,
		FileName: fileName,
		Dir:      opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(opt.SSTableMaxSz),
	})
	return t
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

	f := t.ss.f

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

	//buf := make([]byte, size)
	buf, err := f.Bytes(int(offset), int(size))
	if err != nil {

	}
	//f.ReadAt(buf, int64(offset))

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

func (t *Table) Fid() uint64 {
	return t.fid
}

func (t *Table) Index() *IndexBlock {
	return t.ss.Indexs()
}

func (t *Table) SetIndex(index *IndexBlock) {
	t.ss.indexBlock = index
}

func (t *Table) ReadIndex() (*IndexBlock, error) {

	readPos := len(t.ss.f.Data) - 4
	checksumLen := convert.BytesToU32(t.ss.readCheckError(readPos, 4)) // checksum length
	readPos -= int(checksumLen)
	checksum := t.ss.readCheckError(readPos, int(checksumLen))

	readPos -= 4
	indexLen := convert.BytesToU32(t.ss.readCheckError(readPos, 4))
	readPos -= int(indexLen)

	// read index
	data := t.ss.readCheckError(readPos, int(indexLen))
	if err := codec.VerifyChecksum(data, checksum); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", t.ss.f.Fd.Name())
	}

	index := &IndexBlock{}
	err := proto.Unmarshal(data, index)
	//index := &IndexBlock{
	//	BlockOffsets: make([]*BlockOffset, len(tb.blockList)),
	//	Filter:       nil,
	//	KeyCount:     tb.keyCount,
	//}

	return index, err
}
