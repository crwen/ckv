package version

const (
	kMaxMemCompactLevel = 2
)

type Compaction struct {
	level  int
	inputs [2][]*FileMetaData
}

func (vs *VersionSet) PickLevelForMemTableOutput(smallest, largest []byte) int {
	level := 0
	current := vs.current
	if !vs.overlapInLevel(0, smallest, largest) {

		for ; level < kMaxMemCompactLevel; level++ {
			if vs.overlapInLevel(level+1, smallest, largest) {
				break
			}
			if level+2 >= current.opt.MaxLevelNum {
				break
			}
		}
	}
	return level
}

func (vs *VersionSet) overlapInLevel(level int, smallest, largest []byte) bool {
	current := vs.current
	numFiles := len(current.files[level])
	if numFiles == 0 {
		return false
	}
	cmp := current.opt.Comparable
	if level == 0 {
		for _, meta := range current.files[level] {
			if cmp.Compare(meta.largest, smallest) < 0 ||
				cmp.Compare(meta.smallest, largest) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		idx := current.findFile(current.files[level], smallest)
		if idx >= len(current.files[level]) {
			return false
		}
		if cmp.Compare(largest, current.files[level][idx].smallest) > 0 {
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
