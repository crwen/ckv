package lsm

import (
	"SimpleKV/utils"
)

//Options _
type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each Block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64
}

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	option     *Options

	maxMemFID uint32
}

// NewLSM _
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	lsm.memTable = NewMemTable()
	return lsm
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}

	// TODO 计算内存大小
	if lsm.memTable.Size() > lsm.option.MemTableSize {
		lsm.Rotate()
	}
	if err = lsm.memTable.set(entry); err != nil {
		return err
	}

	// TODO
	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immutables {
		lsm.WriteLevel0Table(immutable)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}

	var (
		entry *utils.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}

	// TODO search sst

	return nil, utils.ErrKeyNotFound
}

// Rotate append MemTable to immutable, and create a new MemTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = NewMemTable()
}

// TODO
func (lsm *LSM) WriteLevel0Table(immutable *memTable) (err error) {
	// 分配一个fid
	return nil
}
