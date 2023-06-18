package version

import (
	"ckv/sstable"
	"ckv/utils"
	"ckv/utils/cmp"
	"sort"
)

type MergeIterator struct {
	list []sstable.TableIterator
	it   utils.Item
	curr sstable.TableIterator
	cmp  cmp.Comparator
}

func NewMergeIterator(iters []sstable.TableIterator, cmp cmp.Comparator) *MergeIterator {
	sort.Slice(iters, func(i, j int) bool {
		return iters[i].GetFID()-iters[j].GetFID() > 0
	})
	return &MergeIterator{
		list: iters,
		cmp:  cmp,
	}
}

func merge(iter1 utils.Iterator, iter2 utils.Iterator) utils.Iterator {
	iter1.Rewind()
	iter2.Rewind()

	return nil
}

func (iter *MergeIterator) Next() {
	var smallest []byte
	k := iter.curr.Item().Entry().Key
	n := 0
	for i := 0; i < len(iter.list); i++ {
		if iter.curr == iter.list[i] {
			iter.list[i].Next()
		}
		for iter.list[i].Valid() && iter.cmp.Compare(iter.list[i].Item().Entry().Key, k) == 0 {
			iter.list[i].Next()
		}
		if iter.list[i].Valid() && smallest == nil {
			smallest = iter.list[i].Item().Entry().Key
			n = i
		} else if iter.list[i].Valid() && iter.cmp.Compare(iter.list[i].Item().Entry().Key, smallest) < 0 {
			smallest = iter.list[i].Item().Entry().Key
			n = i
		}
	}
	var seq uint64
	for i := 0; i < len(iter.list); i++ {

		if iter.list[i].Valid() && smallest == nil {
			smallest = iter.list[i].Item().Entry().Key
			seq = iter.list[i].Item().Entry().Seq
			n = i
		} else if iter.list[i].Valid() && iter.cmp.Compare(iter.list[i].Item().Entry().Key, smallest) <= 0 {
			if iter.list[i].Item().Entry().Seq <= seq {
				continue
			}
			smallest = iter.list[i].Item().Entry().Key
			seq = iter.list[i].Item().Entry().Seq
			n = i
		}
	}
	iter.curr = iter.list[n]
	//iter.list[n].Next()
}

func (iter *MergeIterator) Valid() bool {
	for _, it := range iter.list {
		if it.Valid() {
			return true
		}
	}
	return false
}

func (iter *MergeIterator) Rewind() {
	var key []byte
	for i, it := range iter.list {
		it.Rewind()
		iter.list[i] = it
		if it.Valid() && key == nil {
			key = it.Item().Entry().Key
			iter.curr = it
		} else if it.Valid() && iter.cmp.Compare(it.Item().Entry().Key, key) < 0 {
			key = it.Item().Entry().Key
			iter.curr = it
		}
	}
}

func (iter *MergeIterator) Item() utils.Item {
	return iter.curr.Item()
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
	iter.curr = iter.list[n]
	iter.list[n].Next()
}
