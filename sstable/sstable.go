package sstable

import (
	"io"
	"os"
	"sync"

	"ckv/file"
	"ckv/utils/errs"
)

// SSTable 文件的内存封装
type SSTable struct {
	lock           *sync.RWMutex
	f              *file.MmapFile
	indexBlock     *IndexBlock
	minKey         []byte
	maxKey         []byte
	fileSize       uint64
	fid            uint64
	hasBloomFilter bool
}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}

	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}

func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	errs.Panic(err)
	return buf
}

//func NewSStable(opt *Options) *SSTable {
//	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
//	utils.Err(err)
//	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
//}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

// OpenSStable 打开一个 sst文件
func OpenSStable(opt *file.Options) *SSTable {
	omf, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	errs.Err(err)
	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
}

// Indexs _
func (ss *SSTable) Indexs() *IndexBlock {
	return ss.indexBlock
}

// HasBloomFilter _
func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

// Close 关闭
func (ss *SSTable) Close() error {
	return ss.f.Close()
}

func (ss *SSTable) SetIndex(index *IndexBlock) {
	ss.indexBlock = index
}

func (ss *SSTable) SetMin(key []byte) {
	ss.minKey = key
}

func (ss *SSTable) GetMin() []byte {
	return ss.minKey
}

func (ss *SSTable) SetMax(key []byte) {
	ss.maxKey = key
}

func (ss *SSTable) GetMax() []byte {
	return ss.maxKey
}

func (ss *SSTable) GetName() string {
	return ss.f.Fd.Name()
}

func (ss *SSTable) GetFid() uint64 {
	return ss.fid
}

// Detele _
func (ss *SSTable) Detele() error {
	return ss.f.Delete()
}
