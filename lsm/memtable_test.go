package lsm

import (
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/errs"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestMemTableCreate(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)
	val, err := mem.Get([]byte{1}, 0)
	assert.Nil(t, val)
	assert.Equal(t, err, errs.ErrKeyNotFound)
}

func TestMemTableDestroy(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)
	val, err := mem.Get([]byte{1}, 0)
	assert.Nil(t, val)
	assert.Equal(t, err, errs.ErrKeyNotFound)
	mem.DecrRef()
	assert.Nil(t, mem.table)
}

func TestMemTableDestroy1(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)

	n := 16
	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		mem.Set(e)
	}
	mem.table.PrintSkipList()
}

func TestMemTableUpdate(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)
	mem.Set(&utils.Entry{
		Key:   []byte("123"),
		Value: []byte("123"),
	})
	n := 2000
	for i := 1; i <= n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
			Seq:   uint64(i),
		}
		mem.Set(e)
		v, _ := mem.Get(e.Key, uint64(i))
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)
		assert.Equal(t, uint64(i), v.Seq)
	}
	for i := 1; i <= n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, _ := mem.Get(e.Key, uint64(i))
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)

		assert.Equal(t, uint64(i), v.Seq)

		v, _ = mem.Get(e.Key, uint64(i-1))
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)

		v, _ = mem.Get(e.Key, uint64(i+1))
		assert.Nil(t, v, v)
	}
}

func TestMemTableIterator(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)

	m := make(map[string]string)
	n := 1000
	for i := 1; i <= n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
			Seq:   uint64(i),
		}
		mem.Set(e)
		v, _ := mem.Get(e.Key, 0)
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)
		m[string(e.Key)] = string(e.Value)
	}
	fmt.Println(m)

	iter := mem.NewMemTableIterator()
	iter.Rewind()
	for iter.Valid() {

		entry := iter.Item().Entry()
		v, ok := m[string(entry.Key)]

		assert.Equal(t, true, ok, string(entry.Key))
		assert.Equal(t, v, string(entry.Value))
		iter.Next()
	}
	iter.Close()
	mem.DecrRef()
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	mem := NewMemTable(cmp.ByteComparator{}, nil)

	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := &utils.Entry{Key: key(i), Value: key(i)}
			assert.Nil(t, mem.Set(e))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := mem.Get(key(i), 0)
			assert.Nil(t, err)
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
	const n = 1000
	mem := NewMemTable(cmp.ByteComparator{}, nil)

	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}

	var step = 50
	for i := 0; i < n; i += step {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < step; j++ {
				e := &utils.Entry{Key: key(i + j), Value: key(i + j)}
				assert.Nil(b, mem.Set(e))
			}
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := mem.Get(key(i), 0)
			assert.Nil(b, err)
			if v != nil {
				require.EqualValues(b, key(i), v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}
