package version

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"ckv/file"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/errs"
)

const (
	kMaxMemCompactLevel = 2
)

type CompactStatus struct {
	tables map[uint64]struct{}
	levels []*levelCompactStatus
	sync.RWMutex
}

type levelCompactStatus struct {
	smallest []byte
	largest  []byte
	delSize  int64
}

func NewCompactStatus(option *utils.Options) *CompactStatus {
	cs := &CompactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

type Compaction struct {
	base        []*FileMetaData
	target      []*FileMetaData
	baseLevel   int
	targetLevel int
}

func (vs *VersionSet) RunCompact() int {
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	// select {
	// case <- randomDelay.C:
	<-randomDelay.C
	// TODO close case <- close:

	//}

	ticker := time.NewTicker(3000 * time.Millisecond)
	defer ticker.Stop()
	// for {

	// select {
	// case <- ticker.C:
	<-ticker.C
	vs.compact(1)
	// TODO close case <- close:

	//}
	//}
	return 0
}

func (vs *VersionSet) compact(id int) {
	opt := vs.current.opt
	c := vs.pickCompaction()
	if c == nil || len(c.base)+len(c.target) <= 1 {
		return
	}
	log.Println("Compact begin")
	defer log.Println("Compaction end")

	var iters []sstable.TableIterator
	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		iters = append(iters, t.NewIterator(opt))
	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		// t := sstable.OpenTable(vs.current.opt, id)
		iters = append(iters, t.NewIterator(opt))
	}
	newFid := vs.IncreaseNextFileNumber(1)

	iter := NewMergeIterator(iters, opt.Comparable)
	builder := sstable.NewTableBuiler(opt)
	var entry *utils.Entry

	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		builder.Add(entry, false)

	}
	iter.Close()

	sstName := file.FileNameSSTable(opt.WorkDir, newFid)
	t, err := builder.Flush(sstName)
	// t.MinKey = firstEntry.Key
	if err != nil {
		errs.Panic(err)
	}
	t.MaxKey = entry.Key

	ve := NewVersionEdit()
	ve.RecordAddFileMeta(c.targetLevel, t)

	mergeFids := make([]uint64, 0)
	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		ve.RecordDeleteFileMeta(c.baseLevel, t)
		mergeFids = append(mergeFids, id)
	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		ve.RecordDeleteFileMeta(c.targetLevel, t)
		mergeFids = append(mergeFids, id)
	}

	// TODO: write vgroup to manifest

	// delete
	vs.lock.Lock()
	defer vs.lock.Unlock()

	vs.LogAndApply(ve)
	vs.addFileMeta(c.targetLevel, t)

	vs.info.MergeVLogGroup(mergeFids, newFid)
	vs.info.SetTableState(newFid, NORMAL)

	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		vs.DeleteFileMeta(c.baseLevel, c.targetLevel, t)
		vs.info.SetTableState(id, NORMAL)

		t.DecrRef(nil)
	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		vs.DeleteFileMeta(c.targetLevel, c.targetLevel, t)
		vs.info.SetTableState(id, NORMAL)

		t.DecrRef(nil)

	}

	log.Printf("compact from level %d to level %d. create %s. delete %d files \n",
		c.baseLevel, c.targetLevel, sstName, len(ve.deletes))
}

// pickCompaction method    pick sstables to compact
func (vs *VersionSet) pickCompaction() *Compaction {
	vs.lock.Lock()
	defer vs.lock.Unlock()

	var c Compaction
	c.baseLevel = vs.current.pickCompactionLevel()

	filter := func(file *FileMetaData) bool {
		if state, ok := vs.info.GetTableState(file.id); ok && state == NORMAL {
			return true
		}
		return false
	}
	// compact itself for max level
	// TODO: remove?
	c.base = make([]*FileMetaData, 0)
	c.target = make([]*FileMetaData, 0)
	if c.baseLevel == vs.current.opt.MaxLevelNum-1 {
		for i := range vs.current.files[c.baseLevel] {
			if filter(vs.current.files[c.baseLevel][i]) {
				c.target = append(c.target, vs.current.files[c.baseLevel][i])
			}
		}
		// c.target = filter(vs.current.files[c.baseLevel])

		c.targetLevel = c.baseLevel
		return &c
	}
	// c.base = append(c.base, vs.current.files[c.baseLevel]...)
	// TODO compact to more higher level
	c.targetLevel = c.baseLevel + 1

	var smallest, largest []byte
	cmp := vs.current.opt.Comparable
	if c.baseLevel == 0 {
		for i := range vs.current.files[c.baseLevel] {
			if filter(vs.current.files[c.baseLevel][i]) {
				c.base = append(c.base, vs.current.files[c.baseLevel][i])
			}
		}
		// c.base = append(c.base, vs.current.files[0]...)
		utils.AssertTrue(len(c.base) > 0)
		if len(c.base) > 0 {
			smallest, largest = c.base[0].smallest, c.base[0].largest
		}
		for i := 0; i < len(c.base); i++ {
			f := c.base[i]
			if cmp.Compare(f.largest, largest) > 0 {
				largest = f.largest
			}
			if cmp.Compare(f.smallest, smallest) < 0 {
				smallest = f.smallest
			}
		}
	} else {
		cmp := vs.current.opt.Comparable
		// sort by smallest key
		sort.Slice(vs.current.files[c.baseLevel], func(i, j int) bool {
			return cmp.Compare(vs.current.files[c.baseLevel][i].smallest,
				vs.current.files[c.baseLevel][j].smallest) < 0
		})
		c.base = make([]*FileMetaData, 0)
		c.target = make([]*FileMetaData, 0)
		pendingGC := vs.pendingGC
		if pendingGC != nil {
			if vs.pendingGC.level == c.baseLevel {
				for i := 0; i < len(vs.current.files[c.baseLevel]); i++ {
					meta := vs.current.files[c.baseLevel][i]
					if state, ok := vs.info.GetTableState(meta.id); ok && state == NORMAL && meta.id != vs.pendingGC.sstId {
						c.base = append(c.base, vs.current.files[c.baseLevel][i])
						smallest = vs.current.files[c.baseLevel][i].smallest
						largest = vs.current.files[c.baseLevel][i].largest
						break
					}
				}
			} else if vs.pendingGC.level == c.targetLevel {
				for i := 0; i < len(vs.current.files[c.baseLevel]); i++ {
					meta := vs.current.files[c.baseLevel][i]
					if cmp.Compare(pendingGC.smallest, meta.largest) > 0 ||
						cmp.Compare(pendingGC.largest, meta.smallest) < 0 {
						c.base = append(c.base, vs.current.files[c.baseLevel][i])
						smallest = vs.current.files[c.baseLevel][i].smallest
						largest = vs.current.files[c.baseLevel][i].largest
						break
					}
				}
			}
		}
		if len(c.base) == 0 {
			return &c
		}

		vs.info.SetTableState(c.base[0].id, COMPACTING)

		// append sst that overlap
		// cannot happen
		for i := 0; i < len(vs.current.files[c.baseLevel]); i++ {
			f := vs.current.files[c.baseLevel][i]
			if state, ok := vs.info.GetTableState(f.id); ok && state != NORMAL {
				continue
			}
			// if there are overlap key, append to base
			if cmp.Compare(f.smallest, largest) <= 0 {
				c.base = append(c.base, f)
				largest = f.largest
				panic(fmt.Sprintf("overlapped key between SSTable in level %d", c.baseLevel))
			}
		}
	}
	for i := 0; i < len(vs.current.files[c.targetLevel]); i++ {
		f := vs.current.files[c.targetLevel][i]

		if cmp.Compare(f.largest, smallest) < 0 || cmp.Compare(f.smallest, largest) > 0 {
			continue
		} else {
			c.target = append(c.target, f)
			vs.info.SetTableState(f.id, COMPACTING)
		}
	}
	return &c
}

func (vs *VersionSet) PickLevelForMemTableOutput(smallest, largest []byte) int {
	return vs.current.pickLevelForMemTableOutput(smallest, largest)
}

func (v *Version) pickLevelForMemTableOutput(smallest, largest []byte) int {
	v.vset.lock.RLock()
	defer v.vset.lock.RUnlock()
	level := 0
	if !v.overlapInLevel(0, smallest, largest) {
		for ; level < kMaxMemCompactLevel; level++ {
			if v.overlapInLevel(level+1, smallest, largest) {
				break
			}
			if level+2 >= v.opt.MaxLevelNum {
				break
			}
		}
	}
	return level
}

func (v *Version) overlapInLevel(level int, smallest, largest []byte) bool {
	numFiles := len(v.files[level])
	if numFiles == 0 {
		return false
	}
	cmp := v.opt.Comparable
	if level == 0 {
		for _, meta := range v.files[level] {
			if cmp.Compare(meta.largest, smallest) < 0 ||
				cmp.Compare(meta.smallest, largest) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		idx := v.findFile(v.files[level], smallest)
		if idx >= len(v.files[level]) {
			return false
		}
		if cmp.Compare(largest, v.files[level][idx].smallest) > 0 {
			return true
		}
	}
	return false
}

func (v *Version) findFile(files []*FileMetaData, key []byte) int {
	cmp := v.opt.Comparable
	sort.Slice(files, func(i, j int) bool {
		return cmp.Compare(files[i].smallest, files[j].smallest) < 0
	})
	if len(files) == 0 {
		return 0
	}
	low, high := 0, len(files)-1
	for low < high {
		mid := (high-low)/2 + low
		if cmp.Compare(files[mid].largest, key) >= 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}
	if cmp.Compare(files[low].largest, key) >= 0 {
		return low
	}
	return len(files)
}
