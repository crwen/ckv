package lsm

import (
	"ckv/utils"
	"ckv/utils/cmp"
	"ckv/utils/errs"
	"fmt"
	"github.com/stretchr/testify/assert"
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

func TestMemTableUpdate(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)
	mem.Set(&utils.Entry{
		Key:   []byte("123"),
		Value: []byte("123"),
	})
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
	}
	for i := 1; i <= n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
		}
		v, _ := mem.Get(e.Key, 0)
		assert.NotNil(t, v)
		assert.Equal(t, e.Value, v.Value)
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
