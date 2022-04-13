package lsm

type levelManager struct {
	maxFID uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt    *Options
	levels []*levelHandler
	lsm    *LSM
}

func (lsm *LSM) newLevelManager() *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.opt = lsm.option

	return lm
}
