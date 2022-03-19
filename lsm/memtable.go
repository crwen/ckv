package lsm

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"bytes"
)

const walFileExt string = ".wal"

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemtable _
func (lsm *LSM) NewMemtable() *memTable {
	return nil
}

// Close
func (m *memTable) close() error {

	return nil
}

// set key/value to memtable
func (m *memTable) set(entry *utils.Entry) error {

	return nil
}

// Get get key/value from memtable
func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	return m.sl.Search(key), nil
}

func (m *memTable) Size() int64 {
	return m.sl.Size()
}
