package lsm

import (
	"SimpleKV/utils"
	"sync/atomic"
)

type MemTable struct {
	table *utils.SkipList
	arena *utils.Arena

	// TODO: wal
}

// NewMemtable _
func (lsm *LSM) NewMemTable() *MemTable {
	arena := utils.NewArena(lsm.option.MemTableSize)

	atomic.AddUint64(&(lsm.lm.maxFID), 1)

	return &MemTable{table: utils.NewSkipListWithComparator(arena, lsm.option.Comparable), arena: arena}
}

func (mem MemTable) set(entry *utils.Entry) error {

	mem.table.Add(entry)

	return nil
}

func (mem MemTable) Get(key []byte) (*utils.Entry, error) {

	v := mem.table.Search(key)
	e := &utils.Entry{
		Key:   key,
		Value: v,
	}
	return e, nil
}

func (m *MemTable) Size() int64 {
	return m.table.Size()
}
