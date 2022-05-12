package cache

import "sync"

type LRU struct {
	sync.RWMutex
	m map[string]*Node
	//head     *Node
	//tail     *Node
	list     *List
	capacity int
}

func NewLRUReplacer(capacity int) Replacer {

	return &LRU{
		m:        make(map[string]*Node),
		list:     newList(),
		capacity: capacity,
	}
}

func (lru *LRU) Get(key string) interface{} {
	//lru.RLock()
	//defer lru.RUnlock()
	if node, ok := lru.m[key]; ok {
		lru.list.Remove(node)
		lru.list.Put2Head(node)
		return node.value
	}
	return nil
}

func (lru *LRU) Put(key string, value interface{}) {
	//lru.Lock()
	//defer lru.Unlock()
	if node, ok := lru.m[key]; ok {
		lru.list.Remove(node)
		node.value = value
		lru.list.Put2Head(node)
		return
	}
	if len(lru.m) == lru.capacity {
		removed := lru.list.Remove(lru.list.tail.prev)
		delete(lru.m, removed.key)
	}
	newNode := &Node{
		key:   key,
		value: value,
	}
	lru.list.Put2Head(newNode)
	lru.m[key] = newNode
}
