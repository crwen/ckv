package sstable

import (
	"ckv/utils"
	"fmt"
	"testing"
)

func TestIter(t *testing.T) {
	opt := &utils.Options{
		WorkDir:      "../work_test",
		SSTableMaxSz: 0,
	}
	table := OpenTable(opt, 15)
	iter := table.NewIterator(opt)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		e := iter.Item().Entry()
		fmt.Println(string(e.Key), string(e.Value))
	}
}
