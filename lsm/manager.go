package lsm

import (
	"SimpleKV/cache"
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"SimpleKV/version"
)

type levelManager struct {
	maxFID uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt    *utils.Options
	levels []*levelHandler
	verSet *version.VersionSet
	cache  *cache.Cache
	lsm    *LSM
}

func (lm levelManager) Get(key []byte) (*utils.Entry, error) {
	return lm.levels[0].Get(key)
}

func (lsm *LSM) newLevelManager() *levelManager {
	lm := &levelManager{lsm: lsm}
	lm.opt = lsm.option
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	lm.cache = cache.NewCache(1024, 1024)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*sstable.Table, 0),
			lm:       lm,
		})
	}
	lsm.verSet, _ = version.Open(lm.opt)

	return lm
}
