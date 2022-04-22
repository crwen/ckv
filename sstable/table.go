package sstable

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"SimpleKV/utils/errs"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
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
func (t *Table) Delete() error {
	return t.ss.Detele()
}

func (t *Table) Serach(key []byte) (entry *utils.Entry, err error) {
	iter := t.NewIterator(t.opt)
	//iter.seekToFirst()
	iter.Seek(key)
	//err = iter.err
	//if err != nil {
	//	return nil, err
	//}
	if !iter.Valid() {
		return nil, errs.ErrKeyNotFound
	}
	if t.Compare(iter.Item().Entry().Key, key) == 0 {
		return iter.Item().Entry(), nil
	}

	//index := t.ss.Indexs()
	//filter := utils.Filter(index.Filter)
	//if t.ss.HasBloomFilter() && !filter.MayContainKey(key) {
	//	return nil, errs.ErrKeyNotFound
	//}
	//idx := t.findGreater(index, key)
	//if idx < 0 {
	//	return nil, nil
	//}

	// search block
	//block, err := t.ReadBlock(idx)
	//iter := BlockIterator{}
	//iter.setBlock(block, t.opt.Comparable)
	//iter.seekToFirst()
	//iter.Seek(key)
	//err = iter.Error()
	////if err != nil {
	////	return nil, err
	////}
	//if t.Compare(iter.Item().Entry().Key, key) == 0 {
	//	return iter.Item().Entry(), nil
	//}

	// TODO cache block

	//for i, bo := range block.EntryOffsets {
	//	var k, v []byte
	//	if i == len(block.EntryOffsets)-1 {
	//		k, v = block.readEntry(block.Data[bo:], uint32(block.entriesIndexStart)-bo)
	//	} else {
	//		k, v = block.readEntry(block.Data[bo:], block.EntryOffsets[i+1]-bo)
	//	}
	//
	//	if t.Compare(k, key) == 0 {
	//		return &utils.Entry{Key: k, Value: v}, nil
	//	}
	//}
	return nil, errs.ErrKeyNotFound

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

func (t *Table) ReadBlock(idx int) (*Block, error) {
	if idx < 0 {
		return nil, nil
	}
	block := &Block{}

	f := t.ss.f
	index := t.ss.Indexs()

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
	block.entriesIndexStart = int(offset)
	//buf = buf[:offset]

	// TODO cache block

	return block, nil
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

func (t *Table) Size() uint64 {
	return t.ss.fileSize
}

type TableIterator struct {
	it        utils.Item
	opt       *utils.Options
	t         *Table
	blockPos  int
	blockIter *BlockIterator
	err       error
}

func (iter *TableIterator) GetFID() uint64 {
	return iter.t.fid
}

func (t *Table) NewIterator(options *utils.Options) TableIterator {
	return TableIterator{
		opt:       options,
		t:         t,
		blockIter: &BlockIterator{},
	}
}

func (iter *TableIterator) Next() {
	if iter.blockPos >= len(iter.t.ss.Indexs().BlockOffsets) {
		iter.err = io.EOF
		return
	}
	if len(iter.blockIter.data) == 0 {
		block, err := iter.t.ReadBlock(iter.blockPos)
		if err != nil {
			iter.err = err
			return
		}
		iter.blockIter.setBlock(block, iter.t.opt.Comparable)
		iter.blockIter.seekToFirst()
		iter.err = iter.blockIter.Error()
		return
	}
	iter.blockIter.Next()
	if !iter.blockIter.Valid() {
		// read next block
		iter.blockPos++
		iter.blockIter.data = nil
		iter.Next()
		return
	}
	iter.it = iter.blockIter.it
}

func (iter *TableIterator) Valid() bool {
	return iter.err != io.EOF
}

func (iter *TableIterator) Rewind() {
	iter.seekToFirst()
}

func (iter *TableIterator) Item() utils.Item {
	return iter.it
}

func (iter *TableIterator) Close() error {
	iter.blockIter.Close()
	return nil
}

func (iter *TableIterator) Seek(key []byte) {
	index := iter.t.ss.Indexs()
	filter := utils.Filter(index.Filter)
	if iter.t.ss.HasBloomFilter() && !filter.MayContainKey(key) {
		iter.err = io.EOF
		return
	}
	idx := iter.t.findGreater(index, key)
	if idx < 0 {
		iter.err = io.EOF
		return
	}

	// search block
	block, err := iter.t.ReadBlock(idx)
	iter.blockIter.setBlock(block, iter.t.opt.Comparable)
	iter.blockIter.seekToFirst()
	iter.blockIter.Seek(key)
	err = iter.blockIter.Error()
	if err != nil {
		//	iter.err = err
		//	return
	}
	iter.it = iter.blockIter.it
}

func (iter *TableIterator) seekToFirst() {
	numBlocks := len(iter.t.ss.Indexs().BlockOffsets)
	if numBlocks == 0 {
		iter.err = io.EOF
		return
	}
	iter.blockPos = 0
	block, err := iter.t.ReadBlock(iter.blockPos)
	if err != nil {
		iter.err = err
		return
	}
	iter.blockIter.setBlock(block, iter.t.opt.Comparable)
	iter.blockIter.seekToFirst()
	iter.it = iter.blockIter.Item()
	iter.err = iter.blockIter.Error()
}
