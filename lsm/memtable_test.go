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
	assert.Equal(t, err, errs.ErrEmptyKey)
}

func TestMemTableUpdate(t *testing.T) {
	mem := NewMemTable(cmp.ByteComparator{}, nil)
	mem.Set(&utils.Entry{
		Key:   []byte("123"),
		Value: []byte("123"),
	})
	n := 100
	for i := 1; i <= n; i++ {
		e := &utils.Entry{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("%d", i)),
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
