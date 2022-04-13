package lsm

import (
	"SimpleKV/utils"
	"bytes"
	"fmt"
	"testing"
)

var (
	// 初始化opt
	opt = &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       1 << 14, // 16K
		MemTableSize:       1 << 14, // 16K
		BlockSize:          1 << 10, // 1K
		BloomFalsePositive: 0,
	}
)

func TestLSM_Set(t *testing.T) {
	lsm := NewLSM(opt)

	e := &utils.Entry{
		Key:       []byte("CRTS😁数据库MrGSBtL12345678"),
		Value:     []byte("KV入门"),
		ExpiresAt: 123,
	}
	lsm.Set(e)

	for i := 1; i < 500; i++ {
		e := utils.BuildEntry()
		lsm.Set(e)
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
