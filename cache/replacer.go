package cache

import (
	"sync"
)

type Replacer interface {
	Get(key string) interface{}
	Put(key string, value interface{})
}

type LRU struct {
	sync.RWMutex
	m        map[string]*Node
	head     *Node
	tail     *Node
	capacity int
}

type Node struct {
	key   string
	value interface{}
	next  *Node
	prev  *Node
}

func NewLRUReplacer(capacity int) Replacer {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head
	return &LRU{
		m:        make(map[string]*Node),
		head:     head,
		tail:     tail,
		capacity: capacity,
	}
}

func (lru *LRU) Get(key string) interface{} {
	lru.RLock()
	defer lru.RUnlock()
	if node, ok := lru.m[key]; ok {
		lru.remove(node)
		lru.put2Head(node)
		return node.value
	}
	return nil
}

func (lru *LRU) Put(key string, value interface{}) {
	lru.Lock()
	defer lru.Unlock()
	if node, ok := lru.m[key]; ok {
		lru.remove(node)
		node.value = value
		lru.put2Head(node)
		return
	}
	if len(lru.m) == lru.capacity {
		removed := lru.remove(lru.tail.prev)
		delete(lru.m, removed.key)
	}
	newNode := &Node{
		key:   key,
		value: value,
	}
	lru.put2Head(newNode)
	lru.m[key] = newNode
}

func (lru *LRU) remove(node *Node) *Node {
	prev := node.prev
	next := node.next
	prev.next = next
	next.prev = prev
	node.prev = nil
	node.next = nil
	return node
}

func (lru *LRU) put2Head(node *Node) {
	next := lru.head.next
	node.next = next
	next.prev = node
	node.prev = lru.head
	lru.head.next = node
}
