package version

import (
	"SimpleKV/cache"
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/utils/convert"
	"SimpleKV/utils/errs"
	"bufio"
	"os"
	"path/filepath"
	"sort"
)

const (
	VersionEdit_CREATE = 0
	VersionEdit_DELETE = 1
)

type VersionSet struct {
	nextFileNumber     uint64
	manifestFileNumber uint64
	logNumber          uint64

	head       *Version
	current    *Version
	tableCache *cache.Cache
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
	vs := &VersionSet{}
	current := NewVersion(opt)
	current.vset = vs
	current.f = f
	vs.current = current
	vs.head = &Version{}
	vs.tableCache = cache.NewCache(100, 100)

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

	for {
		buf := make([]byte, 16)
		_, err := r.Read(buf)
		if err != nil {
			break
		}
		op := convert.BytesToU16(buf[0:2])
		level := convert.BytesToU16(buf[2:4])
		fm := &FileMetaData{}
		fm.id = convert.BytesToU64(buf[4:12])
		ssz := convert.BytesToU32(buf[12:])
		smallest := make([]byte, ssz)
		_, err = r.Read(smallest)
		if err != nil {
			break
		}
		fm.smallest = smallest

		buf = make([]byte, 4)
		_, err = r.Read(buf)
		if err != nil {
			break
		}
		lsz := convert.BytesToU32(buf)
		largest := make([]byte, lsz)
		_, err = r.Read(largest)
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
			current.files[level][fm.id] = fm
		case VersionEdit_DELETE:
			delete(current.files[level], fm.id)
		}
	}

	//for _, data := range current.files[0] {
	//	fmt.Println(data.id, string(data.smallest), string(data.largest))
	//}

}

func (vs *VersionSet) Add(level int, t *sstable.Table) {

	vs.current.files[level][t.Fid()] = &FileMetaData{
		id:       t.Fid(),
		largest:  t.MaxKey,
		smallest: t.MinKey,
	}
	vs.tableCache.AddIndex(t.Fid(), t.Index())
}
func (vs *VersionSet) GetIndex(fid uint64) *sstable.IndexBlock {
	return vs.tableCache.GetIndex(fid)
}
func (vs *VersionSet) AddIndex(fid uint64, index *sstable.IndexBlock) {
	vs.tableCache.AddIndex(fid, index)
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
	if entry, err := vs.searchL0SST(key); err == nil && entry != nil {
		return entry, nil
	}
	// TODO serach LN
	return nil, nil
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

//func (vs *VersionSet) Get(key []byte) (*utils.Entry, error) {
//current := vs.current
//for _, meta := range current.files[0]{
//	if meta {
//
//	}
//}
//}
