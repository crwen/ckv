package lsm

import (
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"SimpleKV/utils/errs"
	"SimpleKV/utils/file"
)

var (
	comparator cmp.Comparator = cmp.ByteComparator{}
)

type LSM struct {
	memTable   *MemTable
	immutables []*MemTable
	option     *utils.Options
	lm         *levelManager

	maxMemFID uint32
}

// NewLSM _
func NewLSM(opt *utils.Options) *LSM {
	if opt.Comparable != nil {
		comparator = opt.Comparable
	} else {
		opt.Comparable = cmp.ByteComparator{}
	}
	lsm := &LSM{option: opt}
	lsm.lm = lsm.newLevelManager()
	lsm.memTable = lsm.NewMemTable()
	return lsm
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return errs.ErrEmptyKey
	}

	// TODO 计算内存大小
	if lsm.memTable.Size() > lsm.option.MemTableSize {
		lsm.Rotate()
	}
	if err = lsm.memTable.set(entry); err != nil {
		return err
	}

	// TODO
	// check immutables
	for _, immutable := range lsm.immutables {
		lsm.WriteLevel0Table(immutable)
		immutable.table.Close()
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*MemTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, errs.ErrEmptyKey
	}

	var (
		entry *utils.Entry
		err   error
	)
	// serach from memtable first
	if entry, err = lsm.memTable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}

	// search from immutable, beginning at the newest immutable
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}

	return lsm.lm.Get(key)
}

// WriteLevel0Table write immutable to sst file
func (lsm *LSM) WriteLevel0Table(immutable *MemTable) (err error) {
	// 分配一个fid
	//fid := mem.wal.Fid()
	fid := lsm.lm.maxFID
	sstName := file.FileNameSSTable(lsm.option.WorkDir, fid)

	// 构建一个 builder
	builder := sstable.NewTableBuiler(lsm.option)
	iter := immutable.table.NewIterator()
	var entry *utils.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		builder.Add(entry, false)
	}

	t, err := builder.Flush(sstName)
	t.MaxKey = entry.Key
	if err != nil {
		errs.Panic(err)
	}

	// TODO update manifest

	lsm.lm.levels[0].add(t)

	return
}

// Rotate append MemTable to immutable, and create a new MemTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}

func Compare(a, b []byte) int {
	return comparator.Compare(a, b)
}
