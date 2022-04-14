package lsm

import (
	"SimpleKV/utils"
	"SimpleKV/utils/file"
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

	MaxLevelNum int
}

type LSM struct {
	memTable   *MemTable
	immutables []*MemTable
	option     *Options
	lm         *levelManager

	maxMemFID uint32
}

// NewLSM _
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	lsm.lm = lsm.newLevelManager()
	lsm.memTable = lsm.NewMemTable()
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
		return nil, utils.ErrEmptyKey
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

	// TODO search sst
	return nil, utils.ErrKeyNotFound
}

// WriteLevel0Table write immutable to sst file
func (lsm *LSM) WriteLevel0Table(immutable *MemTable) (err error) {
	// 分配一个fid
	//fid := mem.wal.Fid()
	fid := lsm.lm.maxFID
	sstName := file.FileNameSSTable(lsm.option.WorkDir, fid)

	// 构建一个 builder
	builder := newTableBuiler(lsm.option)
	iter := immutable.table.NewIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}
	builder.flush(lsm.lm, sstName)
	// 创建一个 table 对象
	//table := NewTable(lm, sstName, builder)
	//err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
	//	ID:       fid,
	//	Checksum: []byte{'m', 'o', 'c', 'k'},
	//})
	// manifest写入失败直接panic
	//utils.Panic(err)
	// 更新manifest文件
	//lsm.lm.levels[0].add(table)

	return
}

// Rotate append MemTable to immutable, and create a new MemTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}
