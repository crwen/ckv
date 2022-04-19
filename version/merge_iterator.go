package version

import (
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"fmt"
)

type MergeIterator struct {
	list []utils.Iterator
	it   utils.Item
	cmp  cmp.Comparator
}

func NewMergeIterator(iters []utils.Iterator, cmp cmp.Comparator) *MergeIterator {
	return &MergeIterator{
		list: iters,
		cmp:  cmp,
	}
	//switch len(iters) {
	//case 0:
	//	return &MergeIterator{}
	//case 1:
	//	return iters[0]
	//case 2:
	//
	//	return merge(iters[0], iters[1])
	//}
	//mid := len(iters) / 2
	//return NewMergeIterator(
	//	[]utils.Iterator{
	//		NewMergeIterator(iters[:mid]),
	//		NewMergeIterator(iters[mid:]),
	//	})
}

func merge(iter1 utils.Iterator, iter2 utils.Iterator) utils.Iterator {
	iter1.Rewind()
	iter2.Rewind()
	//for ; iter1.Valid() && iter2.Valid();  {
	//	entry1 := iter1.Item().Entry()
	//	entry2 := iter2.Item().Entry()
	//
	//
	//	iter1.Next()
	//	iter2.Next()
	//}
	return nil
}

func (iter *MergeIterator) Next() {
	n := 0
	var key []byte
	for i, it := range iter.list {
		if it.Valid() && iter.cmp.Compare(it.Item().Entry().Key, key) > 0 {
			n = i
		}
	}
	if iter.list[n].Valid() {
		iter.it = iter.list[n].Item()
		iter.list[n].Next()
	} else {
		fmt.Println("!valid")
	}
	//iter.it = iter.list[n].Item()
}

func (iter *MergeIterator) Valid() bool {
	for _, it := range iter.list {
		if it.Valid() {
			return true
		}
	}
	return false
	//if iter.Valid() {
	//	return true
	//}
	//return false
}

func (iter *MergeIterator) Rewind() {
	for _, it := range iter.list {
		it.Rewind()
	}
}

func (iter *MergeIterator) Item() utils.Item {
	return iter.it
}

func (iter *MergeIterator) Close() error {
	for _, it := range iter.list {
		it.Close()
	}
	return nil
}

func (iter *MergeIterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (iter *MergeIterator) seekToFirst() {
	for i := 0; i < len(iter.list); i++ {
		iter.list[i].Rewind()
	}
	n := 0
	var key []byte
	for i, it := range iter.list {
		if it.Valid() && iter.cmp.Compare(it.Item().Entry().Key, key) > 0 {
			n = i
		}
	}
	iter.it = iter.list[n].Item()
	iter.list[n].Next()
}
