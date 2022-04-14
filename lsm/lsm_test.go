package lsm

import (
	"SimpleKV/utils"
	"bytes"
	"fmt"
	"os"
	"testing"
)

var (
	// 初始化opt
	opt = &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       1 << 14, // 16K
		MemTableSize:       1 << 12, // 16K
		BlockSize:          1 << 10, // 1K
		BloomFalsePositive: 0,
	}
)

func TestLSM_Set(t *testing.T) {
	clearDir()
	lsm := NewLSM(opt)

	e := &utils.Entry{
		Key:       []byte("CRTS😁数据库MrGSBtL12345678"),
		Value:     []byte("KV入门"),
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
	lsm := NewLSM(opt)

	e := &utils.Entry{
		Key:       []byte("CRTS😁数据库MrGSBtL12345678"),
		Value:     []byte("KV入门"),
		ExpiresAt: 123,
	}
	lsm.Set(e)

	for i := 1; i < 100; i++ {
		e := utils.BuildEntry()
		lsm.Set(e)
		if v, err := lsm.Get(e.Key); err != nil {
			panic(err)
		} else if !bytes.Equal(e.Value, v.Value) {
			err = fmt.Errorf("lsm.Get(e.Key) value not equal !!!")
			panic(err)
		}
	}
	for i := 1; i < 100; i++ {
		e := utils.BuildEntry()
		if v, err := lsm.Get(e.Key); err != nil {
			panic(err)
		} else if !bytes.Equal(e.Value, v.Value) {
			err = fmt.Errorf("lsm.Get(e.Key) value not equal !!!")
			panic(err)
		}
	}
	fmt.Println(lsm.memTable.Size() / 1024)
	//if v, err := lsm.Get(e.Key); err != nil {
	//	panic(err)
	//} else if !bytes.Equal(e.Value, v.Value) {
	//	err = fmt.Errorf("lsm.Get(e.Key) value not equal !!!")
	//	panic(err)
	//}

}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
