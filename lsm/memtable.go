package lsm

import "SimpleKV/utils"

type memTable struct {
	table *utils.SkipList
	arena *utils.Arena

	// TODO: wal
}

func NewMemTable() *memTable {
	arena := utils.NewArena()
	return &memTable{
		table: utils.NewSkipList(arena),
		arena: arena,
	}
}

func (mem memTable) set(entry *utils.Entry) error {

	mem.table.Add(entry)

	return nil
}

func (mem memTable) Get(key []byte) (*utils.Entry, error) {

	v := mem.table.Search(key)
	e := &utils.Entry{
		Key:   key,
		Value: v,
	}
	return e, nil
}

func (m *memTable) Size() int64 {
	return m.table.Size()
}
