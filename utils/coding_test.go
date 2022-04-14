package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestCoding(t *testing.T) {
	arena := NewArena(1 << 20)

	for i := 0; i < 10000; i++ {
		key := []byte("LSMDB@数据库afadfa" + fmt.Sprintf("%06d", i))
		value := []byte("SimpleKV@入门adfasf" + fmt.Sprintf("%06d", i))

		keySize := len(key)
		valSize := len(value)
		internalKeySize := keySize + 8
		encodedLen := VarintLength(uint64(internalKeySize)) +
			internalKeySize + VarintLength(uint64(valSize)) + valSize

		offset := arena.Allocate(uint32(encodedLen))

		w := arena.PutKey(key, offset)

		arena.PutVal(value, offset+w)

		k, _ := arena.getKey(offset)
		v, _ := arena.getVal(offset + w)
		assert.Equal(t, key, k)
		assert.Equal(t, value, v)

	}
	fmt.Println(unsafe.Sizeof(Node{}))
	fmt.Println(unsafe.Sizeof([]Node{}))
	fmt.Println(unsafe.Sizeof(&Node{}))
	fmt.Println("================")
	fmt.Println(unsafe.Sizeof(Key{}))
	fmt.Println(unsafe.Sizeof(Value{}))
	fmt.Println(unsafe.Sizeof(float64(0)))
	fmt.Println((123 + uint32(nodeAlign)) & ^uint32(nodeAlign))

}
