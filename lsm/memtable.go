package lsm

import (
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/codec"
	"ckv/utils/convert"
	"ckv/utils/errs"
	"fmt"
	"path/filepath"
	"sync/atomic"
)

type Table = utils.SkipList

type InternalComparator struct {
	userComparator cmp.Comparator
}

func (cmp InternalComparator) Compare(a, b []byte) int {
	return cmp.userComparator.Compare(a, b)
}

func newInternalComparator(comparator cmp.Comparator) InternalComparator {
	return InternalComparator{userComparator: comparator}
}

func getKey(data []byte) []byte {
	if len(data) < 8 {
		return nil
	}
	return data[8:]
}

func getSeq(data []byte) uint64 {
	if len(data) < 8 {
		return 0
	}
	return convert.BytesToU64(data[:8])
}

type MemTable struct {
	table      *Table
	comparator cmp.Comparator
	wal        *WalFile
	ref        int32
	state      int32
}

// NewMemtable _
func NewMemTable(comparator cmp.Comparator, wal *WalFile) *MemTable {
	arena := utils.NewArena()

	//newFid := atomic.AddUint64(&(lsm.maxFID), 1)
	//newFid := lsm.IncreaseFid(1)
	//fileOpt := &file.Options{
	//	FID:      newFid,
	//	FileName: mtFilePath(lsm.option.WorkDir, newFid),
	//	Dir:      lsm.option.WorkDir,
	//	Flag:     os.O_CREATE | os.O_RDWR,
	//	MaxSz:    int(lsm.option.MemTableSize),
	//}
	m := &MemTable{
		//table: utils.NewSkipListWithComparator(arena, newInternalComparator(comparator)),
		table:      utils.NewSkipListWithComparator(arena, comparator),
		wal:        wal,
		comparator: comparator,
		state:      NORMAL,
	}
	m.IncrRef()
	return m
}

func (mem *MemTable) Set(entry *utils.Entry) error {

	// write wal first
	if mem.wal != nil {
		if err := mem.wal.Write(entry); err != nil {
			return err
		}
	}

	//  ------------------------    ---------------------
	// |  key_size | key | tag |   | value_size | value |
	//  -----------------------    ---------------------
	key_size := len(entry.Key)
	// val_size := len(entry.Value)
	internal_key_size := key_size + 8

	encoded_len := codec.VarintLength(uint64(internal_key_size)) + internal_key_size

	buf := make([]byte, encoded_len)

	off := codec.EncodeVarint32(buf, uint32(internal_key_size))
	copy(buf[off:], entry.Key)
	off += len(entry.Key)

	//codec.EncodeVarint64(buf[off:], (entry.Seq<<8)|0x1)
	copy(buf[off:], convert.U64ToBytes(entry.Seq<<8|0x1))
	off += 8
	mem.table.Add(buf, entry.Value)

	// write MemTable
	// key, val := entry.Key, entry.Value
	//val = append(val, convert.U64ToBytes(entry.Seq|0x1)...)
	// key = append(convert.U64ToBytes(entry.Seq), key...)
	// mem.table.Add(key, val)

	return nil
}

func (mem *MemTable) Get(key []byte, seq uint64) (*utils.Entry, error) {
	// codec.VarintLength(uint64(internal_key_size)) + internal_key_size

	var (
		//userKeyOff int
		tagOff int
	)
	internal_key_size := len(key) + 8
	buf := make([]byte, codec.VarintLength(uint64(internal_key_size))+internal_key_size)
	off := codec.EncodeVarint32(buf, uint32(internal_key_size))
	//userKeyOff = off

	copy(buf[off:], key)
	off += len(key)
	tagOff = off
	//codec.EncodeVarint64(buf[off:], (seq<<8)|0x1)
	//copy(buf[off:], convert.U64ToBytes(0|0x1))

	// off := codec.EncodeVarint32(buf, codec.VarintLength(uint64(internal_key_size)))

	// internalKey := append(convert.U64ToBytes(seq), key...)
	//fmt.Println(string(buf))
	//v := mem.table.Search(buf)
	it := mem.table.NewIterator()
	defer it.Close()
	it.Seek(buf)
	if it.Valid() && len(it.Key()) > 8 {
		if mem.comparator.Compare(buf[:tagOff], it.Key()[:len(it.Key())-8]) != 0 {
			return nil, errs.ErrKeyNotFound
		}
		v := &utils.Entry{
			Key:   parseKey(it.Key()),
			Value: it.Value(),
			Seq:   parseSeq(it.Key()),
		}
		return v, nil
	}
	return nil, errs.ErrKeyNotFound
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
	m.table = nil
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
	m.IncrRef()
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
	item := m.list.Item()
	entry := item.Entry()

	entry.Seq = parseSeq(entry.Key)
	entry.Key = parseKey(entry.Key)
	return entry
}

func (m MemTableIterator) Close() error {
	//m.list.Close()
	m.mem.DecrRef()
	return nil
}

func (m MemTableIterator) Seek(key []byte) {
	m.list.Seek(key)
}

func parseKey(internalKey []byte) []byte {
	keySz := codec.DecodeVarint32(internalKey[0:4])
	off := codec.VarintLength(uint64(keySz))
	return internalKey[off : len(internalKey)-8]
}

func parseSeq(internalKey []byte) uint64 {
	return convert.BytesToU64(internalKey[len(internalKey)-8:]) >> 8
}
