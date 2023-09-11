package sstable

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"ckv/file"
	"ckv/utils"
	"ckv/utils/codec"
	"ckv/utils/convert"
	"ckv/utils/errs"
	"ckv/vlog"
)

// sst 的内存形式
type Table struct {
	ss           *SSTable
	opt          *utils.Options
	MinKey       []byte
	MaxKey       []byte
	pendingVlogs []uint64
	fid          uint64
	sync.RWMutex
	ref int32
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
	t.IncrRef()
	return t
}

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *Table) DecrRef(fn func() error) error {
	newRef := atomic.AddInt32(&t.ref, -1)

	if newRef == 0 {
		if fn != nil {
			err := fn()
			if err != nil {
				return err
			}
		}
		if err := t.ss.Detele(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Delete() error {
	// t.Lock()
	// defer t.Unlock()
	return t.ss.Detele()
}

func (t *Table) Rename(filename string) (bool, error) {
	// t.Lock()
	// defer t.Unlock()
	// return t.ss.Detele()
	name := t.ss.f.Fd.Name()
	if !strings.HasSuffix(filename, ".bak") {
		return false, nil
	}
	newName := strings.Replace(name, ".bak", "", 1)

	err := os.Rename(name, newName)
	if err != nil {
		return false, err
	}
	t.ss.f.Sync()

	return true, nil
}

func (t *Table) Serach(key []byte) (entry *utils.Entry, err error) {
	// t.RLock()
	// defer t.RUnlock()

	iter := t.NewIterator(t.opt)
	defer iter.Close()
	// iter.seekToFirst()
	iter.Seek(key)
	//err = iter.err
	//if err != nil {
	//	return nil, err
	//}
	if !iter.Valid() {
		// iter.Close()
		return nil, errs.ErrKeyNotFound
	}
	if t.Compare(iter.Item().Entry().Key, key) == 0 {
		e := iter.Item().Entry()
		tag := e.Value[0]
		if tag == utils.VAL {
			e.Value = e.Value[1:]
		} else {
			// val ptr
			fid := convert.BytesToU64(e.Value[1:])
			pos := convert.BytesToU32(e.Value[9:])
			vlog := openVLog(t.opt, fid)
			val, err := vlog.ReadAt(pos)
			if err != nil {
				return nil, err
			}
			e.Value = val
			vlog.Close()
		}
		// iter.Close()
		return e, nil
	}

	// TODO cache block

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

func (t *Table) readBlock(idx int) (*Block, error) {
	if idx < 0 {
		return nil, nil
	}
	block := &Block{}

	f := t.ss.f
	index := t.ss.Indexs()

	blockOffset := index.BlockOffsets[idx]
	offset := blockOffset.Offset
	size := blockOffset.Len

	// buf := make([]byte, size)
	buf, err := f.Bytes(int(offset), int(size))
	if err != nil {
	}
	// f.ReadAt(buf, int64(offset))

	block.Offset = int(offset)
	block.Data = buf

	offset = block.readEntryOffsets(buf)
	block.entriesIndexStart = int(offset)
	// buf = buf[:offset]

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

	return index, err
}

func (t *Table) Size() uint64 {
	return t.ss.fileSize
}

type TableIterator struct {
	it        utils.Item
	err       error
	opt       *utils.Options
	t         *Table
	blockIter *BlockIterator
	blockPos  int
}

func (iter *TableIterator) GetFID() uint64 {
	return iter.t.fid
}

func (t *Table) NewIterator(options *utils.Options) TableIterator {
	// t.RLock()
	t.IncrRef()
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
		block, err := iter.t.readBlock(iter.blockPos)
		if err != nil {
			iter.err = err
			return
		}
		iter.blockIter.setBlock(block, iter.t.opt.Comparable)
		iter.blockIter.seekToFirst()
		iter.err = iter.blockIter.Error()
		iter.it = iter.blockIter.it
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
	// iter.t.RUnlock()
	iter.blockIter.Close()
	return iter.t.DecrRef(nil)
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
	if idx > 0 && iter.t.Compare(index.BlockOffsets[idx].Key, key) == 0 {
		// seek prev block first
		block, err := iter.t.readBlock(idx - 1)
		iter.blockIter.setBlock(block, iter.t.opt.Comparable)
		iter.blockIter.seekToFirst()
		iter.blockIter.Seek(key)
		err = iter.blockIter.Error()
		if err != nil {
			//	iter.err = err
			//	return
		}
		if iter.t.Compare(iter.blockIter.it.Entry().Key, key) == 0 {
			iter.it = iter.blockIter.it
			return
		}
	}

	// search block
	block, err := iter.t.readBlock(idx)
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
	block, err := iter.t.readBlock(iter.blockPos)
	if err != nil {
		iter.err = err
		return
	}
	iter.blockIter.setBlock(block, iter.t.opt.Comparable)
	iter.blockIter.seekToFirst()
	iter.it = iter.blockIter.Item()
	iter.err = iter.blockIter.Error()
}

func openVLog(opt *utils.Options, fid uint64) *vlog.VLogFile {
	fileOpt := &file.Options{
		FID:      fid,
		FileName: filepath.Join(opt.WorkDir, fmt.Sprintf("%05d%s", fid, utils.VLOG_FILE_EXT)),
		Dir:      opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(opt.MemTableSize),
	}
	return vlog.OpenVLogFile(fileOpt)
}
