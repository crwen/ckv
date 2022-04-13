package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestSkipListAdd(t *testing.T) {
	list := NewSkipList(NewArena())
	key, val := "", ""
	maxTime := 20
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("%d", i), fmt.Sprintf("%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry)
		//list.Add(entry)
		assert.Equal(t, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(t, searchVal, []byte(val))
	}
	list.PrintSkipList()
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList(NewArena())

	//Put & Get
	entry1 := NewEntry([]byte("Key1"), []byte("Val1"))
	assert.Nil(t, list.Add(entry1))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key))

	entry2 := NewEntry([]byte("Key2"), []byte("Val2"))
	assert.Nil(t, list.Add(entry2))
	assert.Equal(t, entry2.Value, list.Search(entry2.Key))

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("noexist")))

	//Update a entry
	entry2_new := NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, list.Add(entry2_new))
	list.PrintSkipList()

	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key))

}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList(NewArena())
	key, val := "", ""
	maxTime := 10000
	for i := 0; i < maxTime; i++ {
		//number := rand.Intn(10000)
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := NewEntry([]byte(key), []byte(val))
		res := list.Add(entry)
		assert.Equal(b, res, nil)
		searchVal := list.Search([]byte(key))
		assert.Equal(b, searchVal, []byte(val))
	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList(NewArena())
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(t, l.Add(NewEntry(key(i), key(i))))
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
				require.EqualValues(t, key(i), v)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList(NewArena())
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.Nil(b, l.Add(NewEntry(key(i), key(i))))
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
				require.EqualValues(b, key(i), v)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList(NewArena())
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		v := []byte(fmt.Sprintf("%05d", i))
		list.Add(&Entry{Key: key, Value: v})
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), list.Search(key))
	}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%05d", i))
		assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), list.Search(key))
	}
	//list.PrintSkipList()

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
	iter.Next()

	//for i := 0; iter.Valid(); i++ {
	//	next := iter.Item()
	//	assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), next.Entry().Value)
	//	assert.Equal(t, []byte(fmt.Sprintf("%05d", i)), next.Entry().Key)
	//	iter.Next()
	//}
	fmt.Println(list.arena.size() / 1024.0)

}
