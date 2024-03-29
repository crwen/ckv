package lsm

import (
	"ckv/file"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/convert"
	"ckv/utils/errs"
	"ckv/version"
	"ckv/vlog"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	IMMUTABLE  = -1
	NORMAL     = 0
	COMPACTING = 1
)

var (
	comparator cmp.Comparator = cmp.ByteComparator{}
)

type LSM struct {
	memTable   *MemTable
	immutables []*MemTable
	wal        *WalFile
	option     *utils.Options
	//lm         *levelManager
	verSet *version.VersionSet
	seq    uint64
	//maxFID uint64
	lock                  *sync.RWMutex
	cond                  *sync.Cond
	bgCompactionScheduled bool
	compactState          *version.CompactStatus
}

// NewLSM _
func NewLSM(opt *utils.Options) *LSM {
	if opt.Comparable != nil {
		comparator = opt.Comparable
	} else {
		opt.Comparable = cmp.ByteComparator{}
	}
	lsm := &LSM{option: opt, lock: &sync.RWMutex{}}
	lsm.cond = sync.NewCond(lsm.lock)
	lsm.verSet, _ = version.Open(lsm.option)
	//lsm.compactState = version.NewCompactStatus(lsm.option)
	//lsm.lm = lsm.newLevelManager()
	// recovery
	lsm.memTable, lsm.immutables = lsm.recovery()
	//lsm.memTable = lsm.NewMemTable()
	go lsm.verSet.RunCompact()
	go lsm.verSet.RunGC()
	return lsm
}

func (lsm *LSM) IncreaseFid(delta uint64) uint64 {
	//newFid := atomic.AddUint64(&(lsm.maxFID), delta)

	return lsm.verSet.IncreaseNextFileNumber(delta)
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return errs.ErrEmptyKey
	}

	// write wal first

	// TODO 计算内存大小
	if lsm.memTable.Size() > lsm.option.MemTableSize {
		lsm.rotate()
	}
	sequence := atomic.AddUint64(&lsm.seq, 1)
	entry.Seq = sequence
	if err = lsm.memTable.Set(entry); err != nil {
		return err
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
		seq   = atomic.LoadUint64(&lsm.seq)
	)
	// serach from memtable first
	if entry, err = lsm.memTable.Get(key, seq); entry != nil && entry.Value != nil {
		return entry, err
	}

	// search from immutable, beginning at the newest immutable
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key, seq); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	return lsm.verSet.Get(key)
	//return lsm.lm.Get(key)
}

// WriteLevel0Table write immutable to sst file
func (lsm *LSM) WriteLevel0Table(immutable *MemTable) (err error) {
	//if !atomic.CompareAndSwapInt32(&immutable.state, IMMUTABLE, COMPACTING) {
	//	return nil
	//}
	// 分配一个fid
	//fid := mem.wal.Fid()
	fid := immutable.wal.Fid()
	sstName := file.FileNameSSTable(lsm.option.WorkDir, fid)
	//fmt.Println(fid)
	// 构建一个 builder
	builder := sstable.NewTableBuiler(lsm.option)
	//iter := immutable.table.NewIterator()
	iter := immutable.NewMemTableIterator()
	defer iter.Close()

	vlog := lsm.openVLog(fid, true)

	var entry *utils.Entry
	var firstEntry *utils.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if firstEntry == nil {
			firstEntry = iter.Item().Entry()
		}
		entry = iter.Item().Entry()
		var val []byte
		if len(entry.Value) > utils.SP_THRESHOLD {
			pos := vlog.Pos()
			if err := vlog.Write(entry); err != nil {
				return err
			}
			val = make([]byte, 1+8+4) // tag + fid + off
			vlog.Fid()
			val[0] = utils.VAL_PTR
			off := copy(val[1:], convert.U64ToBytes(vlog.Fid())) + 1
			copy(val[off:], convert.U32ToBytes(pos))
		} else {
			val = make([]byte, len(entry.Value)+1)
			val[0] = utils.VAL
			copy(val[1:], entry.Value)
		}
		entry.Value = val
		builder.Add(entry, false)
	}

	vlog.Close()
	t, err := builder.Flush(sstName)
	t.MaxKey = entry.Key
	//t.MinKey = firstEntry.Key
	if err != nil {
		errs.Panic(err)
	}

	//level := 0
	level := lsm.verSet.PickLevelForMemTableOutput(t.MinKey, t.MaxKey)

	lsm.verSet.AddFileMetaWithGroup(level, t)

	return
}

// rotate append MemTable to immutable, and create a new MemTable
func (lsm *LSM) rotate() {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	for true {
		if lsm.memTable.Size() <= lsm.option.MemTableSize {
			break
		} else if len(lsm.immutables) != 0 {
			lsm.cond.Wait()
		} else {
			lsm.immutables = append(lsm.immutables, lsm.memTable)
			wal := lsm.openWal()
			lsm.memTable = NewMemTable(lsm.option.Comparable, wal)
			lsm.maybeScheduleCompaction()
		}
	}

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
		os.Remove(file.FileNameVLog(lsm.option.WorkDir, fid))
	}
	if maxFID > lsm.verSet.NextFileNumber {
		lsm.verSet.NextFileNumber = maxFID
	}
	for _, imm := range imms {
		lsm.WriteLevel0Table(imm)
	}
	lsm.immutables = lsm.immutables[:0]
	wal := lsm.openWal()
	return NewMemTable(lsm.option.Comparable, wal), imms[:0]
}

func (lsm *LSM) openWal() *WalFile {
	newFid := lsm.IncreaseFid(1)
	fileOpt := &file.Options{
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	return OpenWalFile(fileOpt)
}

func (lsm *LSM) openVLog(fid uint64, delete bool) *vlog.VLogFile {

	fileOpt := &file.Options{
		FID:      fid,
		FileName: mtvFilePath(lsm.option.WorkDir, fid),
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
	}
	return vlog.OpenVLogFile(fileOpt)
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
	arena := utils.NewArena()
	mt := &MemTable{
		table: utils.NewSkipListWithComparator(arena, lsm.option.Comparable),
	}
	mt.wal = OpenWalFile(fileOpt)
	//oldSeq := lsm.seq
	seq, _ := mt.wal.Iterate(mt.recoveryMemTable(lsm.option))
	//atomic.CompareAndSwapUint64(&lsm.seq, oldSeq, seq)
	//atomic.AddUint64(&lsm.seq, seq - lsm.seq)
	lsm.seq = seq
	return mt, nil
}

func Compare(a, b []byte) int {
	return comparator.Compare(a, b)
}

func (lsm *LSM) maybeScheduleCompaction() {
	if lsm.bgCompactionScheduled {
		return
	}
	lsm.bgCompactionScheduled = true
	go lsm.backgroundCall()

}

func (lsm *LSM) backgroundCall() {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	lsm.backgroundCompaction()
	lsm.bgCompactionScheduled = false
	lsm.cond.Broadcast()
}

func (lsm *LSM) backgroundCompaction() {
	imms := lsm.immutables
	lsm.lock.Unlock()
	for _, imm := range imms {
		lsm.WriteLevel0Table(imm)
		imm.DecrRef()
	}
	lsm.immutables = lsm.immutables[:0]

	lsm.lock.Lock()
}

func (lsm *LSM) compactMem() {

	for _, imm := range lsm.immutables {
		lsm.WriteLevel0Table(imm)
	}
	lsm.immutables = lsm.immutables[:0]
	lsm.bgCompactionScheduled = false
	lsm.cond.Broadcast()
}
