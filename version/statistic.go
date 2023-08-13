package version

import (
	"log"
	"sync"
)

type tableState int

const (
	NORMAL     tableState = 1
	COMPACTING tableState = 2
	GC         tableState = 3
	REWRITE    tableState = 4
	DROP       tableState = 5
)

type Statistic struct {
	vlogGroup       *VLogGroup
	tableStatus     *TableStatus
	vlogTableStatus *TableStatus
}

func NewStatistic() *Statistic {
	return &Statistic{
		vlogGroup:       newVLogGroup(),
		tableStatus:     newTableStatus(),
		vlogTableStatus: newTableStatus(),
	}
}

type TableStatus struct {
	statusMap map[uint64]tableState
	sync.RWMutex
}

func newTableStatus() *TableStatus {
	return &TableStatus{
		statusMap: make(map[uint64]tableState),
		RWMutex:   sync.RWMutex{},
	}
}

func (ts *TableStatus) getTableState(fid uint64) (tableState, bool) {
	ts.RLock()
	defer ts.RUnlock()
	if state, ok := ts.statusMap[fid]; !ok {
		return -1, false
	} else {
		return state, true
	}
}

func (ts *TableStatus) setTableState(fid uint64, state tableState) {
	ts.Lock()
	defer ts.Unlock()
	ts.statusMap[fid] = state
}

type VLogGroup struct {
	group  map[uint64][]uint64
	vgfids map[uint64]uint64
	sync.RWMutex
}

func newVLogGroup() *VLogGroup {
	return &VLogGroup{
		group:   make(map[uint64][]uint64),
		vgfids:  make(map[uint64]uint64),
		RWMutex: sync.RWMutex{},
	}
}

// get_ get group of fid
func (vg *VLogGroup) get(fid uint64) ([]uint64, bool) {
	vg.RLock()
	defer vg.RUnlock()
	if group, ok := vg.vgfids[fid]; ok {
		v, b := vg.group[group]
		return v, b
	}
	return nil, false
}

func (vg *VLogGroup) getGroup(fid uint64) (uint64, bool) {
	vg.RLock()
	defer vg.RUnlock()
	if g, ok := vg.vgfids[fid]; ok {
		return g, ok
	}
	return 0, false
}

func (vg *VLogGroup) pickGroupForMerge() ([]uint64, uint64) {
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
	return vg.group[maxGroup], maxGroup
}

// mergeGroup move fids to a new group
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

func (vg *VLogGroup) addNewGroupWith(newGroup uint64) {
	vg.Lock()
	defer vg.Unlock()

	vg.group[newGroup] = []uint64{newGroup}
	vg.vgfids[newGroup] = newGroup
}

func (vg *VLogGroup) print() {
	vg.RLock()
	defer vg.RUnlock()
	for k, v := range vg.group {
		log.Println("group: ", k, v)
	}
}

// deleteFromGroup delete fids from group
func (vg *VLogGroup) deleteFromGroup(removed []uint64) {
	if len(removed) == 0 {
		return
	}
	vg.Lock()
	defer vg.Unlock()
	if g, ok := vg.vgfids[removed[0]]; !ok {
		panic("Target group doesn't exist")
	} else {
		var count int
		for i := range removed {
			removeFid := removed[i]
			delete(vg.vgfids, removeFid)
			fids := vg.group[g]
			for j := range fids {
				if fids[j] == removeFid {
					fids = append(fids[0:j], fids[j+1:]...)
					count += 1
					break
				}
			}
			vg.group[g] = fids
		}
	}

}

func (vg *VLogGroup) moveToGroup(fids []uint64, fid uint64) {

	vg.Lock()
	defer vg.Unlock()
	if g, ok := vg.vgfids[fid]; ok {
		// move to group g
		vg.group[g] = append(vg.group[g], fids...)
	} else {
		// create new group
	}
}

func (vg *VLogGroup) updateBelongGroup(fids []uint64, newGroup uint64) {
	vg.vgfids[newGroup] = newGroup
	for i := range fids {
		vg.vgfids[fids[i]] = newGroup
	}
}

func (info *Statistic) Group(fid uint64) (uint64, bool) {
	return info.vlogGroup.getGroup(fid)
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

func (info *Statistic) AddNewVLogWithGroup(fid uint64) {
	info.vlogGroup.addNewGroupWith(fid)
}

func (info *Statistic) RemoveVLogFromGroup(removed []uint64) {
	info.vlogGroup.deleteFromGroup(removed)
}

func (info *Statistic) MoveToGroup(fids []uint64, fid uint64) {
	info.vlogGroup.moveToGroup(fids, fid)
}

func (info *Statistic) PickGroupForMerge() ([]uint64, uint64) {
	return info.vlogGroup.pickGroupForMerge()
}

func (info *Statistic) PrintVLogGroup() {
	info.vlogGroup.print()
}

func (info *Statistic) GetTableState(fid uint64) (tableState, bool) {
	return info.tableStatus.getTableState(fid)
}

func (info *Statistic) SetTableState(fid uint64, state tableState) {

	info.tableStatus.setTableState(fid, state)
}

func (info *Statistic) GetVTableState(fid uint64) (tableState, bool) {
	return info.vlogTableStatus.getTableState(fid)
}

func (info *Statistic) SetVTableState(fid uint64, state tableState) {

	info.vlogTableStatus.setTableState(fid, state)
}
