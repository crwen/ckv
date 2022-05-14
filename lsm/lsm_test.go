package lsm

import (
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
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
		Key:       []byte("TBS😁数据库🐧🐧🐧🐂🍃🐎🏀🍎"),
		Value:     []byte("KV入门◀◘◙█Ε｡.:*❉ﾟ･*:.｡.｡.:*･゜❆ﾟ･*｡.:*❉ﾟ･*:.｡.｡.★═━┈┈ ☆══━━─－－　☆══━━─－"),
		ExpiresAt: 123,
	}
	lsm.Set(e)

	for i := 1; i < 100; i++ {
		e := utils.BuildEntry()
		lsm.Set(e)
		v, err := lsm.Get(e.Key)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, e.Value, v.Value)
	}
	fmt.Println(lsm.memTable.Size() / 1024)
}

func TestLSM_CRUD(t *testing.T) {
	clearDir()
	comparable := cmp.IntComparator{}
	opt.Comparable = comparable
	lsm := NewLSM(opt)

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		lsm.Set(e)
	}
	//for i := 0; i < 5000; i++ {
	//	e := &utils.Entry{
	//		Key:   []byte(fmt.Sprintf("%d", i)),
	//		Value: []byte(fmt.Sprintf("%d", i+1)),
	//	}
	//	lsm.Set(e)
	//}

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, err := lsm.Get(e.Key)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, e.Value, v.Value)
	}

}

func TestLSM_C(t *testing.T) {
	clearDir()
	comparable := cmp.IntComparator{}
	opt.Comparable = comparable
	lsm := NewLSM(opt)
	var wg sync.WaitGroup
	wg.Add(5)

	adder := func(begin, end int, wg *sync.WaitGroup) {
		defer wg.Done()
		for i := begin; i < end; i++ {
			e := &utils.Entry{
				Key:   []byte(fmt.Sprintf("%d", i)),
				Value: []byte(fmt.Sprintf("%d", i)),
			}
			lsm.Set(e)
		}
	}
	go adder(0, 1000, &wg)
	go adder(1000, 2000, &wg)
	go adder(2000, 3000, &wg)
	go adder(3000, 4000, &wg)
	go adder(2500, 5000, &wg)

	wg.Wait()

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, err := lsm.Get(e.Key)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, e.Value, v.Value)
	}
}

func TestWAL(t *testing.T) {
	clearDir()
	lsm := NewLSM(opt)

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		lsm.Set(e)
	}
	for i := 0; i < 5000; i++ {
		ee := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, err := lsm.Get(ee.Key)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, ee.Value, v.Value)
	}
}

// run
func TestLWAL_Read(t *testing.T) {
	clearDir()
	TestWAL(t)
	lsm := NewLSM(opt)
	ee := &utils.Entry{
		Key:   []byte(fmt.Sprintf("%d", 1111)),
		Value: []byte(fmt.Sprintf("%d", 1111)),
	}
	lsm.Set(ee)

	for i := 0; i < 5000; i++ {
		e := &utils.Entry{
			//Key:   []byte(fmt.Sprintf("%0128d", i)),
			Key: []byte(fmt.Sprintf("%d", i)),
			//Value: []byte(fmt.Sprintf("%0128d", i+1)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, err := lsm.Get(e.Key)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(v.Key), string(v.Value), v.Seq)
		assert.Equal(t, e.Value, v.Value)
	}
	v, err := lsm.Get(ee.Key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v.Key), string(v.Value), v.Seq)

}

func TestCompactiont(t *testing.T) {
	clearDir()
	comparable := cmp.IntComparator{}
	opt.Comparable = comparable
	lsm := NewLSM(opt)
	//go lsm.verSet.RunCompact()

	for i := 0; i < 10000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%0128d", i)),
			Value: []byte(fmt.Sprintf("%0128d", i)),
		}
		lsm.Set(e)
	}
	for i := 0; i < 10000; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%0128d", i)),
			Value: []byte(fmt.Sprintf("%0128d", i+1)),
		}
		lsm.Set(e)
	}

}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
