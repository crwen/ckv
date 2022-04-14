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
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}
	return lm
}
