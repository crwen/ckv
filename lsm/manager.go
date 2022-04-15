package lsm

import (
	"SimpleKV/sstable"
	"SimpleKV/utils"
)

type levelManager struct {
	maxFID uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt    *utils.Options
	levels []*levelHandler
	lsm    *LSM
}

func (lm levelManager) Get(key []byte) (*utils.Entry, error) {
	return lm.levels[0].Get(key)
}

func (lsm *LSM) newLevelManager() *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.opt = lsm.option
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*sstable.Table, 0),
			lm:       lm,
		})
	}
	return lm
}
