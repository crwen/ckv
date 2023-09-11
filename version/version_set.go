package version

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"ckv/cache"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/convert"
	"ckv/utils/errs"
)

const (
	VersionEdit_CREATE      = 0
	VersionEdit_DELETE      = 1
	VersionEdit_BEGIN       = 2
	VersionEdit_END         = 3
	VersionEdit_BEGIN_MAGIC = "BEGIN_MAGIC"
	VersionEdit_END_MAGIC   = "END_MAGIC"
)

type VersionSet struct {
	head               *Version
	current            *Version
	tableCache         *cache.Cache
	info               *Statistic
	pendingGC          *VFileMetaData
	NextFileNumber     uint64
	manifestFileNumber uint64
	logNumber          uint64
	lock               sync.RWMutex
}

func Open(opt *utils.Options) (*VersionSet, error) {
	path := filepath.Join(opt.WorkDir, ManifestFilename)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	vs := NewVersionSet(opt)
	if err != nil {
		if !os.IsNotExist(err) {
			return vs, err
		}

		return vs, nil
	}
	vs.current.f = f
	vs.Replay()

	return vs, err
}

func NewVersionSet(opt *utils.Options) *VersionSet {
	manifestPath := filepath.Join(opt.WorkDir, ManifestFilename)
	vmanifestPath := filepath.Join(opt.WorkDir, VManifestFilename)
	f, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
	if err != nil {
		return nil
	}
	// vs := &VersionSet{Lock: sync.RWMutex{}}

	current := NewVersion(opt)
	current.f = f
	vf, err := os.OpenFile(vmanifestPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o666)
	if err != nil {
		return nil
	}
	current.vf = vf

	vs := &VersionSet{
		NextFileNumber:     0,
		manifestFileNumber: 0,
		logNumber:          0,
		head:               &Version{},
		current:            current,
		tableCache:         cache.NewCache(100, 100),
		info:               NewStatistic(),
		lock:               sync.RWMutex{},
	}
	current.vset = vs

	return vs
}

func (vs *VersionSet) LogAndApply(ve *VersionEdit) {
	vs.current.logBegin()
	for _, tableMeta := range ve.adds {
		vs.current.log(tableMeta.level, tableMeta.f, VersionEdit_CREATE)
	}
	for _, tableMeta := range ve.deletes {
		vs.current.log(tableMeta.level, tableMeta.f, VersionEdit_DELETE)
	}
	vs.current.logEnd()
}

func (vs *VersionSet) VLogAndApply(ve *VersionEdit) {
	for _, tableMeta := range ve.adds {
		vs.current.vlog(tableMeta.level, tableMeta.f, VersionEdit_CREATE)
	}
	for _, tableMeta := range ve.deletes {
		vs.current.vlog(tableMeta.level, tableMeta.f, VersionEdit_DELETE)
	}
}

func (vs *VersionSet) Replay() {
	current := vs.current
	r := bufio.NewReader(current.f)
	var maxFid uint64
	var adds, deletes [][]*FileMetaData
	begin, end := false, false

	for {
		for {
			flag := false
			op, err := r.ReadByte()
			if err != nil {
				break
			}
			switch op {
			case VersionEdit_BEGIN:
				if begin || end {
					flag = true
					break
				}
				if err := current.checkBeginLog(r); err != nil {
					flag = true
					break
				}
				begin = true
				adds = make([][]*FileMetaData, vs.current.opt.MaxLevelNum)
				deletes = make([][]*FileMetaData, vs.current.opt.MaxLevelNum)
			case VersionEdit_END:
				if err := current.checkEndLog(r); err != nil {
					flag = true
					break
				}
				end = true
			default:
				if !begin || end {
					flag = true
					break
				}
				buf := make([]byte, 14)
				_, err := io.ReadFull(r, buf)
				if err != nil {
					flag = true
					break
				}
				level := convert.BytesToU16(buf[0:2])
				fm := &FileMetaData{}
				fm.id = convert.BytesToU64(buf[2:10])
				ssz := convert.BytesToU32(buf[10:])
				if fm.id > maxFid {
					maxFid = fm.id
				}
				smallest := make([]byte, ssz)
				_, err = io.ReadFull(r, smallest)
				if err != nil {
					flag = true
					break
				}
				fm.smallest = smallest

				buf = make([]byte, 4)
				_, err = io.ReadFull(r, buf)
				if err != nil {
					flag = true
					break
				}
				lsz := convert.BytesToU32(buf)
				largest := make([]byte, lsz)
				_, err = io.ReadFull(r, largest)
				if err != nil {
					flag = true
					break
				}
				fm.largest = largest
				switch op {
				case VersionEdit_CREATE:
					adds[level] = append(adds[level], fm)
				case VersionEdit_DELETE:
					deletes[level] = append(deletes[level], fm)
				}
			}
			if flag || (end && begin) {
				break
			}
		}

		if !begin || !end {
			break
		}
		begin, end = false, false

		for i := range adds {
			if adds[i] != nil {
				current.files[i] = append(current.files[i], adds[i]...)
			}
		}
		for i := range deletes {
			if deletes[i] != nil {
				for j := range deletes[i] {
					current.deleteFile(uint16(i), deletes[i][j])
				}
			}
		}
	}

	vs.NextFileNumber = maxFid
}

func (vs *VersionSet) AddFileMetaWithGroup(level int, t *sstable.Table) {
	vs.lock.Lock()
	defer vs.lock.Unlock()

	ve := NewVersionEdit()
	ve.RecordAddFileMeta(level, t)
	vs.LogAndApply(ve)

	vs.addFileMeta(level, t)
	vs.AddNewVLogGroup(t.Fid())
}

func (vs *VersionSet) addFileMeta(level int, t *sstable.Table) {
	meta := &FileMetaData{
		id:       t.Fid(),
		largest:  t.MaxKey,
		smallest: t.MinKey,
		fileSize: t.Size(),
	}
	vs.current.files[level] = append(vs.current.files[level], meta)
	vs.current.vfiles[level] = append(vs.current.vfiles[level], &VFileGroupMetaData{
		sstId: meta.id,
		vfids: make([]uint64, 0),
	})
	vs.tableCache.AddIndex(t.Fid(), t.Index())
}

func (vs *VersionSet) DeleteFileMeta(level, targetLevel int, t *sstable.Table) {
	var vfileMeta *VFileGroupMetaData
	for i := 0; i < len(vs.current.files[level]); i++ {
		if vs.current.files[level][i].id == t.Fid() {
			vs.current.files[level] = append(vs.current.files[level][0:i], vs.current.files[level][i+1:]...)
			// delete from old level
			vfileMeta = vs.current.vfiles[level][i]
			vs.current.vfiles[level] = append(vs.current.vfiles[level][0:i], vs.current.vfiles[level][i+1:]...)
			break
		}
	}
	for i := 0; i < len(vs.current.vfiles[targetLevel]); i++ {
		if vs.current.vfiles[targetLevel][i].sstId == t.Fid() {
			vs.current.vfiles[targetLevel][i].vfids = append(vs.current.vfiles[targetLevel][i].vfids, vfileMeta.vfids...)
			break
		}
	}
	// vs.tableCache.AddIndex(t.Fid(), t.Index())
}

func (vs *VersionSet) FindTable(fid uint64) *sstable.Table {
	table := vs.tableCache.GetTable(fid)
	if table == nil {
		table = sstable.OpenTable(vs.current.opt, fid)
		vs.tableCache.AddTable(fid, table)
	}
	index := vs.tableCache.GetIndex(fid)
	if index == nil {
		if idx, err := table.ReadIndex(); err != nil {
			panic(err)
		} else {
			index = idx
		}
	}
	table.SetIndex(index)
	return table
}

func (vs *VersionSet) Get(key []byte) (*utils.Entry, error) {
	vs.lock.RLock()
	defer vs.lock.RUnlock()
	if entry, err := vs.searchL0SST(key); err == nil && entry != nil {
		return entry, nil
	}
	// TODO serach LN

	if entry, err := vs.searchLNSST(key); err == nil && entry != nil {
		return entry, nil
	}
	return &utils.Entry{Key: key, Value: nil}, nil
}

func (vs *VersionSet) searchL0SST(key []byte) (*utils.Entry, error) {
	var target []uint64
	cmp := vs.current.opt.Comparable
	for _, fileMeta := range vs.current.files[0] {
		if cmp.Compare(fileMeta.smallest, key) <= 0 && cmp.Compare(fileMeta.largest, key) >= 0 {
			target = append(target, fileMeta.id)
		}
	}
	sort.Slice(target, func(i, j int) bool {
		return target[i] > target[j]
	})

	for i := 0; i < len(target); i++ {
		fid := target[i]
		table := vs.FindTable(fid)
		if entry, err := table.Serach(key); err == nil && entry != nil {
			return entry, nil
		}
	}

	return nil, errs.ErrKeyNotFound
}

func (vs *VersionSet) searchLNSST(key []byte) (*utils.Entry, error) {
	current := vs.current
	// cmp := current.opt.Comparable
	for level := 1; level < current.opt.MaxLevelNum; level++ {
		idx := current.findFile(current.files[level], key)
		if idx >= len(current.files[level]) {
			continue
		}
		meta := current.files[level][idx]
		table := vs.FindTable(meta.id)
		if entry, err := table.Serach(key); err == nil && entry != nil {
			return entry, nil
		}
	}
	return nil, errs.ErrKeyNotFound
}

func (vs *VersionSet) IncreaseNextFileNumber(delta uint64) uint64 {
	newFid := atomic.AddUint64(&(vs.NextFileNumber), delta)
	return newFid
}

func (vs *VersionSet) AddNewVLogGroup(fid uint64) {
	vs.info.AddNewVLogWithGroup(fid)
	vs.info.SetTableState(fid, NORMAL)
}
