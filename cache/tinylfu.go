package cache

import (
	"github.com/dgryski/go-metro"
	"sync"
)

type TinyLFU struct {
	sync.RWMutex
	m         map[string]*Node
	cmSketch  *cmSketch
	list      *List
	capacity  int
	threshold int32
	w         int32
}

func NewTinyLFU(capacity int) *TinyLFU {

	return &TinyLFU{
		RWMutex:   sync.RWMutex{},
		m:         make(map[string]*Node),
		cmSketch:  newCmSketch(int64(capacity)),
		list:      newList(),
		capacity:  capacity,
		threshold: int32(capacity),
	}
}

func (lfu *TinyLFU) Get(key string) interface{} {
	lfu.RLock()
	defer lfu.RUnlock()
	if node, ok := lfu.m[key]; ok {
		lfu.cmSketch.Increment(keyToHash(key))
		nd := lfu.findNearer(node)
		lfu.list.Remove(node)
		lfu.list.InsertAfter(nd, node)
		return node.value
	}
	return nil
}

func (lfu *TinyLFU) Put(key string, value interface{}) {
	lfu.Lock()
	defer lfu.Unlock()
	lfu.w++
	if lfu.w == lfu.threshold {
		lfu.cmSketch.Reset()
		lfu.w = 0
	}
	if node, ok := lfu.m[key]; ok {
		nd := lfu.findNearer(node)
		lfu.list.Remove(node)
		node.value = value
		lfu.list.InsertAfter(nd, node)
		return
	}

	newNode := &Node{
		key:   key,
		value: value,
	}

	if len(lfu.m) == lfu.capacity {

		back := lfu.list.Back()
		if lfu.Allow(back, newNode) {
			lfu.list.Remove(back)
			delete(lfu.m, back.key)

			lfu.list.InsertLast(newNode)
			nd := lfu.findNearer(newNode)
			lfu.list.Remove(newNode)
			lfu.list.InsertAfter(nd, newNode)
			lfu.m[key] = newNode
		}
		return
		//removed := lfu.list.RemoveLast()
		//delete(lfu.m, removed.key)
	}

	lfu.list.InsertLast(newNode)
	nd := lfu.findNearer(newNode)
	lfu.list.Remove(newNode)
	lfu.list.InsertAfter(nd, newNode)

	lfu.m[key] = newNode

}

func (lfu *TinyLFU) findNearer(node *Node) *Node {
	if node.prev == lfu.list.head {
		return lfu.list.head
	}
	nd := node.prev
	for lfu.cmSketch.Estimate(keyToHash(node.key)) >= lfu.cmSketch.Estimate(keyToHash(nd.key)) {
		nd = nd.prev
		if nd == lfu.list.head {
			return lfu.list.head
		}
	}
	return nd
}

func (lfu *TinyLFU) Allow(evict *Node, node *Node) bool {
	if lfu.cmSketch.Estimate(keyToHash(evict.key)) > lfu.cmSketch.Estimate(keyToHash(node.key)) {
		return false
	}
	return true
}

func keyToHash(key string) uint64 {
	return metro.Hash64Str(key, 0)
}
