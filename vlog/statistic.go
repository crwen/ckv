package vlog

import "sync"

type Statistic struct {
	vlogGroup *VLogGroup
}

func NewStatistic() *Statistic {
	return &Statistic{
		vlogGroup: newVLogGroup(),
	}
}

type VLogGroup struct {
	group map[uint64][]uint64
	vfids map[uint64]uint64
	sync.RWMutex
}

func newVLogGroup() *VLogGroup {
	return &VLogGroup{
		group:   make(map[uint64][]uint64),
		vfids:   make(map[uint64]uint64),
		RWMutex: sync.RWMutex{},
	}
}

func (vg *VLogGroup) get(fid uint64) ([]uint64, bool) {
	vg.RLock()
	defer vg.RUnlock()
	if group, ok := vg.vfids[fid]; ok {
		v, b := vg.group[group]
		return v, b
	}
	return nil, false
}

func (vg *VLogGroup) pickGroupForMerge() []uint64 {
	vg.RLock()
	defer vg.RUnlock()
	var (
		maxGroup uint64
		maxCount int
	)
	for g, fids := range vg.group {
		if len(fids) > maxCount {
			maxCount = len(fids)
			maxGroup = g
		}
	}
	return vg.group[maxGroup]
}

func (vg *VLogGroup) mergeGroup(fids []uint64, newGroup uint64) {
	vg.Lock()
	defer vg.Unlock()
	group := make([]uint64, 0)
	for i := range fids {
		oldGroup := fids[i]
		if g, ok := vg.group[oldGroup]; !ok {
			panic("Merged group doesn't exist")
		} else {
			group = append(group, g...)
			delete(vg.group, oldGroup)
		}
	}

	if _, ok := vg.group[newGroup]; ok {
		panic("Target group already exist")
	}
	vg.group[newGroup] = group

	vg.updateBelongGroup(fids, newGroup)
}

func (vg *VLogGroup) deleteFromGroup(removed []uint64) {
	if len(removed) == 0 {
		return
	}
	vg.Lock()
	defer vg.Unlock()
	if g, ok := vg.vfids[removed[0]]; !ok {
		panic("Target group doesn't exist")
	} else {
		for i := range removed {
			removeFid := removed[i]
			delete(vg.vfids, removeFid)
			fids := vg.group[g]
			for j := range fids {
				if fids[j] == removeFid {
					fids = append(fids[0:j], fids[j+1:]...)
					break
				}
			}
			vg.group[g] = fids
		}
	}

}

func (vg *VLogGroup) updateBelongGroup(fids []uint64, newGroup uint64) {
	vg.vfids[newGroup] = newGroup
	for i := range fids {
		vg.vfids[fids[i]] = newGroup
	}
}

func (info *Statistic) GetVLogGroup(group uint64) []uint64 {
	if v, b := info.vlogGroup.get(group); b {
		return v
	}
	return nil
}

func (info *Statistic) MergeVLogGroup(base []uint64, target uint64) {
	info.vlogGroup.mergeGroup(base, target)
}

func (info *Statistic) AddNewVLogGroup(fid uint64) {
	info.vlogGroup.mergeGroup([]uint64{}, fid)
}

func (info *Statistic) RemoveVLogFromGroup(removed []uint64) {
	info.vlogGroup.deleteFromGroup(removed)
}

func (info *Statistic) PickGroupForMerge() []uint64 {
	return info.vlogGroup.pickGroupForMerge()
}
