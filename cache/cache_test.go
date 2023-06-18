package cache

import (
	"ckv/utils"
	"fmt"
	"math/rand"
	"testing"
	"time"
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

func TestTinyLFU(t *testing.T) {
	lru := NewTinyLFU(5)
	lru.Put(fmt.Sprintf("%d", 0), 0)

	for i := 0; i < 3; i++ {
		lru.Get(fmt.Sprintf("%d", 0))
	}
	for i := 0; i < 5; i++ {
		lru.Put(fmt.Sprintf("%d", i), i)
		lru.Get(fmt.Sprintf("%d", i))
	}
	//lru.Put("1", 11)
	lru.Put("5", 5)
	lru.Get(fmt.Sprintf("%d", 5))

	v := lru.Get("1")
	fmt.Println(v)
	utils.AssertTrue(v == nil)

}

func TestWinTinyLFUFlood(t *testing.T) {
	rand.Seed(time.Now().Unix())
	n := 1000
	data := make([]string, 0)
	for i := 0; i < 100; i++ {
		for i := 0; i < n; i++ {
			data = append(data, fmt.Sprintf("%d", i))
		}
	}

	lru := NewLRUReplacer(n / 10)
	lfu := NewTinyLFU(n / 10)
	tiny := NewWinTinyLFU(n / 10)

	execute(data, tiny, "W-TinyLFU")
	execute(data, lfu, "TinyLFU")
	execute(data, lru, "LRU")
}

func TestSparseBursts(t *testing.T) {
	rand.Seed(time.Now().Unix())
	n := 10000
	data := make([]string, 0)
	for i := 0; i < 10; i++ {
		for j := 0; j < n/10; j++ {
			data = append(data, fmt.Sprintf("%d", j))
		}
	}

	for i := 0; i < 5; i++ {
		for j := n; j < n+n/10; j++ {
			data = append(data, fmt.Sprintf("%d", j))

		}
	}

	lru := NewLRUReplacer(n / 10)
	lfu := NewTinyLFU(n / 10)
	tiny := NewWinTinyLFU(n / 10)

	execute(data, tiny, "W-TinyLFU")
	execute(data, lfu, "TinyLFU")
	execute(data, lru, "LRU")
}

func TestSparseBursts2(t *testing.T) {
	rand.Seed(time.Now().Unix())
	n := 1000
	data := make([]string, 0)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			for k := i * n; k < (i+1)*n; k++ {
				data = append(data, fmt.Sprintf("%d", k))
			}
			for k := 0; i < 3; i++ {
				for m := 0; k < m/2; k++ {
					data = append(data, fmt.Sprintf("%d", m))
				}
			}
		}
	}

	lru := NewLRUReplacer(n)
	lfu := NewTinyLFU(n)
	tiny := NewWinTinyLFU(n)

	execute(data, tiny, "W-TinyLFU")
	execute(data, lfu, "TinyLFU")
	execute(data, lru, "LRU")
}

func TestWinTinyLFUHotChange(t *testing.T) {
	rand.Seed(time.Now().Unix())
	n := 1000
	data := make([]string, 0)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			for k := i * n; k < (i+1)*n; k++ {
				data = append(data, fmt.Sprintf("%d", k))
			}
		}
	}

	lru := NewLRUReplacer(n)
	lfu := NewTinyLFU(n)
	tiny := NewWinTinyLFU(n)

	execute(data, tiny, "W-TinyLFU")
	execute(data, lfu, "TinyLFU")
	execute(data, lru, "LRU")
}

func TestWinTinyLFU(t *testing.T) {
	rand.Seed(time.Now().Unix())
	n := 10000
	data := make([]string, 0)
	for i := 0; i < n; i++ {
		num := rand.Intn(n)
		data = append(data, fmt.Sprintf("%d", num))
	}

	tiny := NewWinTinyLFU(n / 10)
	lru := NewLRUReplacer(n / 10)
	lfu := NewTinyLFU(n / 10)

	execute(data, tiny, "W-TinyLFU")
	execute(data, lfu, "TinyLFU")
	execute(data, lru, "LRU")
}

func execute(data []string, replacer Replacer, str string) {
	miss, hit := 0, 0
	for _, key := range data {
		if replacer.Get(key) == nil {
			replacer.Put(key, key)
			miss++
		} else {
			hit++
		}
	}
	hitRate := float64(hit) / float64(miss+hit)
	fmt.Printf("%s: miss: %d, hit: %d,  hit rate: %f\n", str, miss, hit, hitRate)
}
