package lsm

import (
	"SimpleKV/file"
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"SimpleKV/utils/errs"
	"SimpleKV/version"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	comparator cmp.Comparator = cmp.ByteComparator{}
)

type LSM struct {
	memTable   *MemTable
	immutables []*MemTable
	option     *utils.Options
	//lm         *levelManager
	verSet *version.VersionSet
	//maxFID uint64
}

// NewLSM _
func NewLSM(opt *utils.Options) *LSM {
	if opt.Comparable != nil {
		comparator = opt.Comparable
	} else {
		opt.Comparable = cmp.ByteComparator{}
	}
	lsm := &LSM{option: opt}
	lsm.verSet, _ = version.Open(lsm.option)
	//lsm.lm = lsm.newLevelManager()
	// recovery
	lsm.memTable, lsm.immutables = lsm.recovery()
	//lsm.memTable = lsm.NewMemTable()
	return lsm
}

func (lsm *LSM) IncreaseFid(delta uint64) uint64 {
	//newFid := atomic.AddUint64(&(lsm.maxFID), delta)

	return lsm.verSet.Increase(delta)
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
		immutable.close()
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
	return lsm.verSet.Get(key)
	//return lsm.lm.Get(key)
}

// WriteLevel0Table write immutable to sst file
func (lsm *LSM) WriteLevel0Table(immutable *MemTable) (err error) {
	// 分配一个fid
	//fid := mem.wal.Fid()
	fid := immutable.wal.Fid()
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

	//level := 0
	level := lsm.verSet.PickLevelForMemTableOutput(t.MinKey, t.MaxKey)

	// TODO update manifest
	ve := version.NewVersionEdit()
	ve.AddFile(level, t)
	lsm.verSet.LogAndApply(ve)

	lsm.verSet.Add(level, t)
	//lsm.lm.levels[0].add(t)

	return
}

// Rotate append MemTable to immutable, and create a new MemTable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memTable)
	lsm.memTable = lsm.NewMemTable()
}

func (lsm *LSM) recovery() (*MemTable, []*MemTable) {
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		errs.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFID := lsm.verSet.NextFileNumber
	// find wal files
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		sz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:sz-len(walFileExt)], 10, 64)
		if maxFID < fid {
			maxFID = fid
		}
		if err != nil {
			errs.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}
	// sort ase
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	imms := []*MemTable{}
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		errs.CondPanic(err != nil, err)
		if mt.table.Size() == 0 {
			continue
		}
		imms = append(imms, mt)
	}
	lsm.verSet.NextFileNumber = maxFID
	return lsm.NewMemTable(), imms
}

func (lsm *LSM) openMemTable(fid uint64) (*MemTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	//mt := lsm.NewMemTable()
	arena := utils.NewArena(1 << 20)
	mt := &MemTable{
		table: utils.NewSkipListWithComparator(arena, lsm.option.Comparable),
		arena: arena,
	}
	mt.wal = OpenWalFile(fileOpt)
	mt.wal.Iterate(mt.recoveryMemTable(lsm.option))
	return mt, nil
}

func Compare(a, b []byte) int {
	return comparator.Compare(a, b)
}
