package lsm

import (
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var (
	// 初始化opt
	opt = &utils.Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       1 << 14, // 16K
		MemTableSize:       1 << 14, // 16K
		BlockSize:          1 << 10, // 1K
		BloomFalsePositive: 0,
		MaxLevelNum:        7,
	}
)

func TestLSM_Set(t *testing.T) {
	clearDir()
	lsm := NewLSM(opt)

	e := &utils.Entry{
		Key:       []byte("😁数据库🐂🐎"),
		Value:     []byte("KV入门◀◘◙█Ε｡.:*❉ﾟ･*:.｡.｡.:*･゜❆ﾟ･*｡.:*❉ﾟ･*:.｡.｡.★═━┈┈ ☆══━━─－－　☆══━━─－"),
		ExpiresAt: 123,
	}
	lsm.Set(e)

	for i := 1; i < 1000; i++ {
		e := utils.BuildEntry()
		lsm.Set(e)
	}
	fmt.Println(lsm.memTable.Size() / 1024)
}

func TestLSM_CRUD(t *testing.T) {
	clearDir()
	comparable := cmp.ByteComparator{}
	opt.Comparable = comparable
	lsm := NewLSM(opt)

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%04d", i)),
			Value: []byte(fmt.Sprintf("%04d", i)),
		}
		lsm.Set(e)
	}

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%04d", i)),
			Value: []byte(fmt.Sprintf("%04d", i)),
		}
		v, err := lsm.Get(e.Key)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, e.Value, v.Value)
	}
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
