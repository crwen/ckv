package lsm

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"SimpleKV/utils/convert"
	"SimpleKV/utils/errs"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

type Table = utils.SkipList

type MemTable struct {
	table *Table
	arena *utils.Arena
	wal   *WalFile
	ref   int32
	state int32
}

// NewMemtable _
func (lsm *LSM) NewMemTable() *MemTable {
	arena := utils.NewArena(lsm.option.MemTableSize)

	//newFid := atomic.AddUint64(&(lsm.maxFID), 1)
	newFid := lsm.IncreaseFid(1)
	fileOpt := &file.Options{
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	m := &MemTable{
		table: utils.NewSkipListWithComparator(arena, lsm.option.Comparable),
		wal:   OpenWalFile(fileOpt),
		arena: arena,
		state: NORMAL,
	}
	m.IncrRef()
	return m
}

func (mem *MemTable) set(entry *utils.Entry) error {

	// write wal first
	//if err := mem.wal.Write(entry); err != nil {
	//	return err
	//}
	// write MemTable
	key, val := entry.Key, entry.Value
	//val = append(val, convert.U64ToBytes(entry.Seq|0x1)...)
	val = append(convert.U64ToBytes(entry.Seq), val...)
	mem.table.Add(key, val)

	return nil
}

func (mem *MemTable) Get(key []byte) (*utils.Entry, error) {

	v := mem.table.Search(key)
	if v == nil {
		return nil, errs.ErrEmptyKey
	}
	vs := utils.DecodeValue(v.Value)
	e := &utils.Entry{
		Key:   key,
		Value: vs.Value,
		Seq:   vs.Seq,
	}
	return e, nil
}

func (m *MemTable) Size() int64 {
	return m.table.Size()
}

// Close
func (m *MemTable) close() error {
	// close wal first
	if err := m.wal.Close(); err != nil {
		return err
	}
	m.table.Close()
	return nil
}

func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

func (m *MemTable) recoveryMemTable(opt *utils.Options) func(*utils.Entry) error {
	return func(e *utils.Entry) error {
		return m.table.Add(e.Key, e.Value)
	}
}

// IncrRef increase the ref by 1
func (m *MemTable) IncrRef() {
	atomic.AddInt32(&m.ref, 1)
}

// DecrRef decrease the ref by 1. If the ref is 0, close the skip list
func (m *MemTable) DecrRef() {
	newRef := atomic.AddInt32(&m.ref, -1)
	if newRef <= 0 {
		m.close()
	}
}

type MemTableIterator struct {
	list *utils.SkipListIterator
	mem  *MemTable
}

func (m *MemTable) NewMemTableIterator() *MemTableIterator {
	//m.IncrRef()
	return &MemTableIterator{list: m.table.NewIterator(), mem: m}
}

func (m MemTableIterator) Next() {
	m.list.Next()
}

func (m MemTableIterator) Valid() bool {
	return m.list.Valid()
}

func (m MemTableIterator) Rewind() {
	m.list.Rewind()
}

func (m MemTableIterator) Item() utils.Item {
	return m.list.Item()
}

func (m MemTableIterator) Close() error {
	//m.list.Close()
	//m.mem.DecrRef()
	return nil
}

func (m MemTableIterator) Seek(key []byte) {
	m.list.Seek(key)
}
