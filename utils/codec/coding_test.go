package codec

import (
	"testing"
)

func TestCoding(t *testing.T) {
	//arena := utils.NewArena(1 << 20)
	//
	//for i := 0; i < 10000; i++ {
	//	key := []byte("LSMDB@数据库afadfa" + fmt.Sprintf("%06d", i))
	//	value := []byte("SimpleKV@入门adfasf" + fmt.Sprintf("%06d", i))
	//
	//	keySize := len(key)
	//	valSize := len(value)
	//	internalKeySize := keySize + 8
	//	encodedLen := VarintLength(uint64(internalKeySize)) +
	//		internalKeySize + VarintLength(uint64(valSize)) + valSize
	//
	//	offset := arena.Allocate(uint32(encodedLen))
	//
	//	w := arena.PutKey(key, offset)
	//
	//	arena.PutVal(value, offset+w)
	//
	//	k, _ := arena.GetKey(offset)
	//	v, _ := arena.GetVal(offset + w)
	//	assert.Equal(t, key, k)
	//	assert.Equal(t, value, v)
	//
	//}
	//fmt.Println(unsafe.Sizeof(utils.Node{}))
	//fmt.Println(unsafe.Sizeof([]utils.Node{}))
	//fmt.Println(unsafe.Sizeof(&utils.Node{}))
	//fmt.Println("================")
	//fmt.Println(unsafe.Sizeof(utils.Key{}))
	//fmt.Println(unsafe.Sizeof(utils.Value{}))
	//fmt.Println(unsafe.Sizeof(float64(0)))

}
