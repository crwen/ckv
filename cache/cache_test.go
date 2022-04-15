package cache

import (
	"SimpleKV/utils"
	"fmt"
	"testing"
)

func TestLRU(t *testing.T) {
	lru := NewLRUReplacer(5)
	for i := 0; i < 5; i++ {
		lru.Put(fmt.Sprintf("%d", i), i)
	}
	lru.Put("1", 11)
	lru.Put("6", 6)

	v := lru.Get("5")
	utils.AssertTrue(v == nil)
}
