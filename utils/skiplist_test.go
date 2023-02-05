package utils

import (
	"SimpleKV/utils/cmp"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSkipListSingleElement(t *testing.T) {

	list := NewSkipListWithComparator(NewArena(1<<20), cmp.IntComparator{})
	key, val := fmt.Sprintf("%d", 3), fmt.Sprintf("%d", 5)
	entry := NewEntry([]byte(key), []byte(val))
	res := list.Add(entry.Key, entry.Value)
	assert.Equal(t, res, nil)
	list.PrintSkipList()

	searchVal := list.Search([]byte(key))
	assert.NotNil(t, searchVal)
	assert.Equal(t, searchVal.Value, []byte(val))
}

func TestSkipListAdd(t *testing.T) {
	//list := NewSkipList(NewArena(1 << 20))
	list := NewSkipListWithComparator(NewArena(1<<20), cmp.IntComparator{})
	key, val := "", ""
	maxTime := 20
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry.Key, entry.Value)
		//res := list.Add(entry)
		//list.AddFileMeta(entry)
		assert.Equal(t, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(t, searchVal.Value, []byte(val))
	}
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("%d", i), fmt.Sprintf("%d", i+1)
		entry := NewEntry([]byte(key), []byte(val))
		//res := list.Add(entry.Key, entry.Value)
		res := list.Add(entry.Key, entry.Value)
		//res := list.Add(entry)
		//list.AddFileMeta(entry)
		assert.Equal(t, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(t, searchVal.Value, []byte(val))
	}
	list.PrintSkipList()
}

func TestSkipListComparatorAdd(t *testing.T) {
	comparator := cmp.IntComparator{}
	list := NewSkipListWithComparator(NewArena(1<<20), comparator)
	key, val := "", ""
	maxTime := 20
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry.Key, entry.Value)
		//res := list.Add(entry)
		//list.AddFileMeta(entry)
		assert.Equal(t, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(t, searchVal.Value, []byte(val))
	}
	list.PrintSkipList()
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(NewArena(1 << 20))

	//Put & Get
	entry1 := NewEntry([]byte("Key1"), []byte("Val1"))
	assert.Nil(t, list.Add(entry1.Key, entry1.Value))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := NewEntry([]byte("Key2"), []byte("Val2"))
	assert.Nil(t, list.Add(entry2.Key, entry2.Value))
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("noexist")))

	//Update a entry
	entry2_new := NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, list.Add(entry2_new.Key, entry2_new.Value))
	list.PrintSkipList()

	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)

}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(NewArena(1 << 20))
	key, val := "", ""
	maxTime := 1000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("Key%0130d", i), fmt.Sprintf("Val%0130d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry.Key, entry.Value)
		assert.Equal(b, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal.Value, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 10000
	l := NewSkipList(NewArena(1 << 20))
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := NewEntry(key(i), key(i))
			assert.Nil(t, l.Add(e.Key, e.Value))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(t, key(i), v.Value)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 10000
	l := NewSkipList(NewArena(1 << 20))
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := NewEntry(key(i), key(i))
			assert.Nil(b, l.Add(e.Key, e.Value))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Search(key(i))
			if v != nil {
				require.EqualValues(b, key(i), v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(NewArena(1 << 20))
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		v := []byte(fmt.Sprintf("%05d", i))
		//list.Add(&Entry{Key: key, Value: v})
		list.Add(key, v)
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), list.Search(key).Value)
	}
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), list.Search(key).Value)
	}

	iter := list.NewIterator()
	iter.Next()

	for i := 0; iter.Valid(); i++ {
		next := iter.Item()
		key := fmt.Sprintf("%05d", i)
		val := fmt.Sprintf("%05d", i)
		ik := string(next.Entry().Key)
		iv := string(next.Entry().Value)
		//assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), next.Entry().Value)
		assert.Equal(t, key, ik)
		assert.Equal(t, val, iv)
		iter.Next()
	}

	iter.Rewind()

	for i := 0; iter.Valid(); i++ {
		next := iter.Item()
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), next.Entry().Value)
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), next.Entry().Key)
		iter.Next()
	}
	fmt.Println(list.arena.size() / 1024.0)

}
