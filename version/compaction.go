package version

import (
	"SimpleKV/file"
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/utils/errs"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"
)

const (
	kMaxMemCompactLevel = 2
)

type Compaction struct {
	baseLevel   int
	targetLevel int
	base        []*FileMetaData
	target      []*FileMetaData
}

func (vs *VersionSet) RunCompact() int {
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	//select {
	//case <- randomDelay.C:
	<-randomDelay.C
	//TODO close case <- close:

	//}

	ticker := time.NewTicker(3000 * time.Millisecond)
	defer ticker.Stop()
	for {

		//select {
		//case <- ticker.C:
		<-ticker.C
		vs.compact(1)
		//TODO close case <- close:

		//}
	}
	return 0
}

func (vs *VersionSet) compact(id int) {
	vs.lock.Lock()
	vs.lock.Unlock()
	c := vs.pickCompaction()
	if c == nil {
		return
	}
	log.Println("Compact begin")
	defer log.Println("Compaction end")
	var iters []sstable.TableIterator
	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		iters = append(iters, t.NewIterator(vs.current.opt))
	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		//t := sstable.OpenTable(vs.current.opt, id)
		iters = append(iters, t.NewIterator(vs.current.opt))
	}
	iter := NewMergeIterator(iters, vs.current.opt.Comparable)
	builer := sstable.NewTableBuiler(vs.current.opt)
	var entry *utils.Entry
	for iter.seekToFirst(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		builer.Add(entry, false)
	}

	vs.Increase(1)
	sstName := file.FileNameSSTable(vs.current.opt.WorkDir, vs.NextFileNumber)
	t, err := builer.Flush(sstName)
	t.MaxKey = entry.Key
	if err != nil {
		errs.Panic(err)
	}

	ve := NewVersionEdit()
	ve.AddFile(c.targetLevel, t)

	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		ve.DeleteFile(c.baseLevel, t)

	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		ve.DeleteFile(c.baseLevel, t)
	}
	vs.LogAndApply(ve)
	vs.Add(c.targetLevel, t)

	// delete
	for _, meta := range c.base {
		id := meta.id
		t := vs.FindTable(id)
		vs.Delete(c.baseLevel, t)
		t.Delete()
	}
	for _, meta := range c.target {
		id := meta.id
		t := vs.FindTable(id)
		vs.Delete(c.targetLevel, t)
		t.Delete()
	}
	log.Printf("compact from level %d to level %d. create %s. delete %d files ",
		c.baseLevel, c.targetLevel, sstName, len(ve.deletes))
}

func (vs *VersionSet) pickCompaction() *Compaction {
	var c Compaction
	c.baseLevel = vs.current.pickCompactionLevel()
	c.base = append(c.base, vs.current.files[c.baseLevel]...)
	// TODO compact to more higher level
	c.targetLevel = c.baseLevel + 1

	var smallest, largest []byte
	cmp := vs.current.opt.Comparable
	if c.baseLevel == 0 {
		c.base = append(c.base, vs.current.files[0]...)
		for i := 0; i < len(c.base); i++ {
			f := c.base[i]
			if cmp.Compare(f.largest, largest) > 0 {
				largest = f.largest
			}
			if cmp.Compare(f.smallest, smallest) < 0 {
				smallest = smallest
			}
		}
	} else {
		cmp := vs.current.opt.Comparable
		sort.Slice(vs.current.files[c.baseLevel], func(i, j int) bool {
			return cmp.Compare(vs.current.files[c.baseLevel][i].smallest,
				vs.current.files[c.baseLevel][j].smallest) < 0
		})
		smallest = vs.current.files[c.baseLevel][0].smallest
		largest = vs.current.files[c.baseLevel][0].largest
		//for i := 0; i < len(vs.current.files[c.baseLevel]); i++ {
		//f := vs.current.files[prior.baseLevel][i]
		//if vs.current {
		//
		//}
		//}
	}

	for i := 0; i < len(vs.current.files[c.targetLevel]); i++ {
		f := vs.current.files[c.targetLevel][i]

		if cmp.Compare(f.largest, smallest) < 0 || cmp.Compare(f.smallest, largest) > 0 {
			continue
		} else {
			c.target = append(c.target, f)
		}
	}
	fmt.Printf("base: %v\n target: %v\n", c.base, c.target)
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