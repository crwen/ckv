package lsm

import (
	"ckv/file"
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/errs"
	"ckv/vlog"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
)

func createMemTable() *MemTable {
	opt := &file.Options{
		Path:     "../work_test",
		FID:      1,
		MaxSz:    1 << 14,
		Flag:     os.O_CREATE | os.O_RDWR,
		FileName: mtvFilePath("../work_test", 100),
	}
	_, err := os.Stat(opt.Path)
	if err == nil {
		os.RemoveAll(opt.Path)
	}
	os.Mkdir(opt.Path, os.ModePerm)

	return NewMemTable(cmp.ByteComparator{}, nil)

}

func TestMemTableCreate(t *testing.T) {
	mem := createMemTable()
	val, err := mem.Get([]byte{1}, 0)
	assert.Nil(t, val)
	assert.Equal(t, err, errs.ErrKeyNotFound)
}

func TestMemTableCreateMore(t *testing.T) {
	mem := createMemTable()
	for i := 0; i < 3; i++ {
		for j := 0; j < 20; j++ {
			e := &utils.Entry{
				Key:   []byte(fmt.Sprintf("%d", j)),
				Value: []byte(fmt.Sprintf("%d", j+i*100)),
			}
			mem.set(e)
		}
	}
	it := mem.table.NewIterator()
	for it.Rewind(); it.Valid(); it.Next() {
		fmt.Println(string(parseKey(it.Item().Entry().Key)), string(it.Item().Entry().Value))
	}
}

func TestMemTableDestroy(t *testing.T) {
	mem := createMemTable()
	val, err := mem.Get([]byte{1}, 0)
	assert.Nil(t, val)
	assert.Equal(t, err, errs.ErrKeyNotFound)
	mem.DecrRef()
	assert.Nil(t, mem.table)
}

func TestMemTableDestroy1(t *testing.T) {
	mem := createMemTable()

	n := 16
	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
			Seq:   uint64(i),
		}
		mem.Set(e)
	}
	for i := 0; i < n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("abc%d", i)),
			Seq:   uint64(16 + i),
		}
		mem.Set(e)
	}
	mem.table.PrintSkipList()
}

func TestMemTableUpdate(t *testing.T) {
	mem := createMemTable()

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
		assert.Nil(t, v, v)

		v, _ = mem.Get(e.Key, uint64(i+1))
		assert.NotNil(t, v, v)
		assert.Equal(t, e.Value, v.Value)
	}
}

func TestMemTableUpdateDup(t *testing.T) {
	mem := createMemTable()

	n := 1000
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
			Value: []byte(fmt.Sprintf("abcdefghijklmnopqrst%d", i)),
			Seq:   uint64(n + i),
		}
		mem.Set(e)
		v, _ := mem.Get(e.Key, uint64(i+n))
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)
		assert.Equal(t, uint64(i+n), v.Seq)

		v, _ = mem.Get(e.Key, uint64(i-1))
		assert.Nil(t, v, v)

		v, _ = mem.Get(e.Key, uint64(i+1))
		if v == nil {
			fmt.Println(string(e.Key), string(e.Value))
			return
		}
		assert.NotNil(t, v, v)

		assert.NotEqual(t, e.Value, v.Value)
	}
}

func TestMemTableIterator(t *testing.T) {
	mem := createMemTable()

	m := make(map[string]string)
	n := 1000
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
	mem := createMemTable()

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
			v, err := mem.Get(key(i), uint64(i))
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
	mem := createMemTable()

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
			v, err := mem.Get(key(i), uint64(i))
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

func TestVLogIter(t *testing.T) {
	opt := &file.Options{
		Path:     "../work_test",
		FID:      1,
		MaxSz:    1 << 14,
		Flag:     os.O_CREATE | os.O_RDWR,
		FileName: mtvFilePath("../work_test", 15),
	}

	vlog := vlog.OpenVLogFile(opt)
	vlog.Iterate(func(e *utils.Entry) error {
		fmt.Println(string(e.Key), string(e.Value))
		return nil
	})
}

func TestName(t *testing.T) {
	fmt.Println([]byte("BEGIN_MAGIC"))
	fmt.Println([]byte("END_MAGIC"))
}
