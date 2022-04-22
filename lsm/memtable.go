package lsm

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"fmt"
	"os"
	"path/filepath"
)

type MemTable struct {
	table *utils.SkipList
	arena *utils.Arena

	wal *WalFile
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
	return &MemTable{
		table: utils.NewSkipListWithComparator(arena, lsm.option.Comparable),
		wal:   OpenWalFile(fileOpt),
		arena: arena,
	}
}

func (mem *MemTable) set(entry *utils.Entry) error {

	// write wal first
	if err := mem.wal.Write(entry); err != nil {
		return err
	}
	// write MemTable
	mem.table.Add(entry)

	return nil
}

func (mem *MemTable) Get(key []byte) (*utils.Entry, error) {

	v := mem.table.Search(key)
	if v == nil {
		return nil, nil
	}
	e := &utils.Entry{
		Key:   key,
		Value: v.Value,
		Seq:   v.Seq,
	}
	return e, nil
}

func (m *MemTable) Size() int64 {
	return m.table.Size()
}

// Close
func (m *MemTable) close() error {
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
		return m.table.Add(e)
	}
}
