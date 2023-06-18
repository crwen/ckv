package lsm

import (
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/version"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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

	TestCreateSSTable(t)
	opt.Comparable = cmp.IntComparator{}

	lsm := NewLSM(opt)
	e := &utils.Entry{
		Key:   []byte(fmt.Sprintf("%d", 1111)),
		Value: []byte(fmt.Sprintf("%d", 1111)),
	}
	lsm.Set(e)

	var iters []sstable.TableIterator

	table := lsm.verSet.FindTable(uint64(1))
	iters = append(iters, table.NewIterator(lsm.option))
	table = lsm.verSet.FindTable(uint64(2))
	iters = append(iters, table.NewIterator(lsm.option))

	iter := version.NewMergeIterator(iters, opt.Comparable)
	var entry *utils.Entry
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry = iter.Item().Entry()
		fmt.Println(string(entry.Key), string(entry.Value), entry.Seq)
	}
	iter.Close()
}
