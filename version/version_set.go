package version

import (
	"bufio"
	"ckv/cache"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/convert"
	"ckv/utils/errs"
	"ckv/vlog"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	VersionEdit_CREATE = 0
	VersionEdit_DELETE = 1
)

type VersionSet struct {
	NextFileNumber     uint64
	manifestFileNumber uint64
	logNumber          uint64

	head       *Version
	current    *Version
	tableCache *cache.Cache
	info       *vlog.Statistic
	lock       sync.RWMutex
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
	path := filepath.Join(opt.WorkDir, ManifestFilename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil
	}
	vs := &VersionSet{lock: sync.RWMutex{}}
	current := NewVersion(opt)
	current.vset = vs
	current.f = f
	vs.current = current
	vs.head = &Version{}
	vs.tableCache = cache.NewCache(100, 100)
	vs.info = vlog.NewStatistic()
	return vs
}

func (vs *VersionSet) LogAndApply(ve *VersionEdit) {
	for _, tableMeta := range ve.adds {
		vs.current.log(tableMeta.level, tableMeta.f, VersionEdit_CREATE)
	}
	for _, tableMeta := range ve.deletes {
		vs.current.log(tableMeta.level, tableMeta.f, VersionEdit_DELETE)
	}
}
func (vs *VersionSet) Replay() {
	current := vs.current
	r := bufio.NewReader(current.f)
	//io.ReadFull()

	for {
		buf := make([]byte, 16)
		_, err := io.ReadFull(r, buf)
		if err != nil {
			break
		}
		op := convert.BytesToU16(buf[0:2])
		level := convert.BytesToU16(buf[2:4])
		fm := &FileMetaData{}
		fm.id = convert.BytesToU64(buf[4:12])
		ssz := convert.BytesToU32(buf[12:])
		smallest := make([]byte, ssz)
		_, err = io.ReadFull(r, smallest)
		if err != nil {
			break
		}
		fm.smallest = smallest

		buf = make([]byte, 4)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			break
		}
		lsz := convert.BytesToU32(buf)
		largest := make([]byte, lsz)
		_, err = io.ReadFull(r, largest)
		if err != nil {
			break
		}
		fm.largest = largest

		//sz := codec.DecodeVarint32(buf)
		//off := codec.VarintLength(uint64(sz))
		//smallest := make([]byte, sz)
		//copy(smallest, buf[off:])
		//r.Read(smallest[sz-off:])
		//buf = make([]byte, 4)
		//_, err = r.Read(buf)
		//if err != nil {
		//	break
		//}
		//sz = codec.DecodeVarint32(buf)
		//off = codec.VarintLength(uint64(sz))
		//largets := make([]byte, sz)
		//copy(largets, buf[off:])
		//r.Read(largets[sz-off:])
		//fm := &FileMetaData{
		//	id:       id,
		//	largest:  largets,
		//	smallest: smallest,
		//}

		switch op {
		case VersionEdit_CREATE:
			current.files[level] = append(current.files[level], fm)
		case VersionEdit_DELETE:
			current.deleteFile(level, fm)
			//delete(current.files[level], fm.id)
		}
		//fmt.Printf("level %d, op %d, fid:%d, %s %s\n", level, op, fm.id, string(fm.smallest), string(fm.largest))
	}
	//fmt.Println("=================")
	//for i, data := range current.files {
	//	fmt.Printf("level %d has %d files\n", i, len(data))
	//}

}

func (vs *VersionSet) AddFileMeta(level int, t *sstable.Table) {
	vs.lock.Lock()
	defer vs.lock.Unlock()
	meta := &FileMetaData{
		id:       t.Fid(),
		largest:  t.MaxKey,
		smallest: t.MinKey,
		fileSize: t.Size(),
	}
	vs.current.files[level] = append(vs.current.files[level], meta)
	vs.tableCache.AddIndex(t.Fid(), t.Index())
}

func (vs *VersionSet) DeleteFileMeta(level int, t *sstable.Table) {
	for i := 0; i < len(vs.current.files[level]); i++ {
		if vs.current.files[level][i].id == t.Fid() {
			vs.current.files[level] = append(vs.current.files[level][0:i], vs.current.files[level][i+1:]...)
			break
		}
	}
	//vs.tableCache.AddIndex(t.Fid(), t.Index())
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
	//cmp := current.opt.Comparable
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
	vs.info.AddNewVLogGroup(fid)
}

//func (vs *VersionSet) Get(key []byte) (*utils.Entry, error) {
//current := vs.current
//for _, meta := range current.files[0]{
//	if meta {
//
//	}
//}
//}
