package lsm

import (
	"ckv/file"
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/convert"
	"ckv/version"
	"ckv/vlog"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

//create
func TestCreateSSTable(t *testing.T) {
	clearDir()
	opt.Comparable = cmp.IntComparator{}
	lsm := NewLSM(opt)
	for i := 0; i < 3; i++ {
		for j := 0; j <= 200; j++ {
			e := &utils.Entry{
				Key:   []byte(fmt.Sprintf("%d", j)),
				Value: []byte(fmt.Sprintf("%d", j+i*100)),
			}
			lsm.Set(e)
		}
	}

	for i := 0; i <= 200; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i+200)),
		}
		v, _ := lsm.Get(e.Key)
		assert.NotNil(t, v)
		assert.Equal(t, string(e.Value), string(v.Value))
	}

}

func TestMerge(t *testing.T) {
	clearDir()

	opt.Comparable = cmp.IntComparator{}

	lsm := NewLSM(opt)
	for i := 0; i < 4; i++ {
		for j := 0; j <= 200; j++ {
			e := &utils.Entry{
				Key:   []byte(fmt.Sprintf("%d", j)),
				Value: []byte(fmt.Sprintf("%d", j+i*100)),
			}
			lsm.Set(e)
		}
	}

	for i := 0; i <= 200; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i+300)),
		}
		v, _ := lsm.Get(e.Key)
		assert.NotNil(t, v)
		assert.Equal(t, string(e.Value), string(v.Value))
	}
	//e := &utils.Entry{
	//	Key:   []byte(fmt.Sprintf("%d", 1111)),
	//	Value: []byte(fmt.Sprintf("%d", 1111)),
	//}
	//lsm.Set(e)

	var iters []sstable.TableIterator

	table1 := lsm.verSet.FindTable(uint64(1))
	iters = append(iters, table1.NewIterator(lsm.option))
	table2 := lsm.verSet.FindTable(uint64(2))
	iters = append(iters, table2.NewIterator(lsm.option))

	iter := version.NewMergeIterator(iters, opt.Comparable)
	var entry *utils.Entry
	var i = 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		assert.Equal(t, string(entry.Key), fmt.Sprintf("%d", i))
		fmt.Println(string(entry.Key), string(entry.Value), entry.Seq)
		i += 1
	}
	iter.Close()
}

func TestMerge2(t *testing.T) {

	opt.Comparable = cmp.IntComparator{}

	lsm := NewLSM(opt)

	var iters []sstable.TableIterator

	//table1 := lsm.verSet.FindTable(uint64(3))
	//iters = append(iters, table1.NewIterator(lsm.option))
	//table2 := lsm.verSet.FindTable(uint64(4))
	//iters = append(iters, table2.NewIterator(lsm.option))
	//table3 := lsm.verSet.FindTable(uint64(5))
	//table4 := lsm.verSet.FindTable(uint64(6))
	//table5 := lsm.verSet.FindTable(uint64(7))
	//table6 := lsm.verSet.FindTable(uint64(8))
	//table7 := lsm.verSet.FindTable(uint64(9))
	//table8 := lsm.verSet.FindTable(uint64(10))
	//table9 := lsm.verSet.FindTable(uint64(11))
	//table10 := lsm.verSet.FindTable(uint64(12))
	//table11 := lsm.verSet.FindTable(uint64(13))
	table15 := lsm.verSet.FindTable(uint64(15))
	//iters = append(iters, table2.NewIterator(lsm.option))
	//iters = append(iters, table3.NewIterator(lsm.option))
	//iters = append(iters, table4.NewIterator(lsm.option))
	//iters = append(iters, table5.NewIterator(lsm.option))
	//iters = append(iters, table6.NewIterator(lsm.option))
	//iters = append(iters, table7.NewIterator(lsm.option))
	//iters = append(iters, table8.NewIterator(lsm.option))
	//iters = append(iters, table9.NewIterator(lsm.option))
	//iters = append(iters, table10.NewIterator(lsm.option))
	//iters = append(iters, table11.NewIterator(lsm.option))
	iters = append(iters, table15.NewIterator(lsm.option))

	iter := version.NewMergeIterator(iters, opt.Comparable)
	var entry *utils.Entry
	var i = 0

	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		//assert.Equal(t, string(entry.Key), fmt.Sprintf("%d", i))
		var val []byte
		if entry.Value[0] == utils.VAL_PTR {
			fid := convert.BytesToU64(entry.Value[1:])
			pos := convert.BytesToU32(entry.Value[9:])
			vlog := vlog.OpenVLogFile(&file.Options{
				Path:     "../work_test",
				FID:      fid,
				MaxSz:    1 << 14,
				Flag:     os.O_CREATE | os.O_RDWR,
				FileName: mtvFilePath("../work_test", fid),
			})
			v, err := vlog.ReadAt(pos)
			if err != nil {
				panic(err)
			}
			val = make([]byte, len(v))
			copy(val, v)
		} else {
			val = make([]byte, len(entry.Value))
			copy(val, entry.Value)
		}
		fmt.Println("key:", string(entry.Key), " value:", string(val), " seq: ", entry.Seq)
		i += 1
	}
	iter.Close()
}

func TestLSM_CRUD_Compact(t *testing.T) {
	clearDir()
	comparable := cmp.IntComparator{}
	opt.Comparable = comparable
	lsm := NewLSM(opt)

	n := 2000

	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		lsm.Set(e)
	}

	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("abcdefghijklmn%d", i)),
		}
		lsm.Set(e)
		v, err := lsm.Get(e.Key)

		assert.Nil(t, err, string(e.Key))
		assert.Equal(t, e.Value, v.Value, string(v.Value))
	}

	time.Sleep(10000 * time.Millisecond)
	fmt.Println("==============================================")
	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("abcdefghijklmn%d", i)),
		}
		v, err := lsm.Get(e.Key)

		assert.Nil(t, err, string(e.Key))
		assert.Equal(t, e.Value, v.Value, string(v.Value))
	}
}

func TestSearchMerge(t *testing.T) {

	opt.Comparable = cmp.IntComparator{}

	lsm := NewLSM(opt)
	n := 2000
	//for i := 0; i < n; i++ {
	//	e := &utils.Entry{
	//		Key:   []byte(fmt.Sprintf("%d", i)),
	//		Value: []byte(fmt.Sprintf("abcdefghijklmn%d", i)),
	//	}
	//	v, err := lsm.Get(e.Key)
	//
	//	assert.Nil(t, err, string(e.Key))
	//	assert.Equal(t, e.Value, v.Value, string(v.Value))
	//}
	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("abcdefghijklmn%d", i)),
		}
		v, err := lsm.Get(e.Key)
		assert.Nil(t, err, string(e.Key))
		assert.Equal(t, e.Value, v.Value, string(v.Value))
		fmt.Println("key:", string(v.Key), " value:", string(v.Value), " seq: ", v.Seq)

		//assert.Nil(t, err, string(e.Key))
		//assert.Equal(t, e.Value, v.Value, string(v.Value))
	}
}
