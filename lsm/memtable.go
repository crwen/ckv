package lsm

import "SimpleKV/utils"

type MemTable struct {
	table *utils.SkipList
	arena *utils.Arena

	// TODO: wal
}

func (mem MemTable) NewMemTable(arenaSize int) *MemTable {
	arena := utils.NewArena()
	return &MemTable{
		table: utils.NewSkipList(arena),
		arena: arena,
	}
}

func (mem MemTable) set(entry *utils.Entry) error {

	mem.table.Add(entry)

	return nil
}
