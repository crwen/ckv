package utils

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
)

const (
	kMaxHeight = 20
)

type SkipList struct {
	head      *Node
	maxHeight int
	rand      *rand.Rand
	lock      sync.RWMutex
}

type Node struct {
	Entry *Entry
	next  []*Node
	score float64
}

func NewNode(entry *Entry, height int) *Node {

	node := &Node{
		Entry: entry,
		next:  make([]*Node, height),
		score: calcScore(entry.Key),
	}
	return node
}

func NewSkipList(arenaSize int64) *SkipList {
	list := &SkipList{
		head:      NewNode(&Entry{Key: []byte{0}}, kMaxHeight),
		maxHeight: 0,
		rand:      r,
		lock:      sync.RWMutex{},
	}
	return list
}

func (list *SkipList) FindGreaterOrEqual(entry *Entry, prev []*Node) *Node {
	p := list.head
	level := list.GetMaxHeight() - 1

	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			if list.KeyIsAfterNode(entry.Key, next) {
				if list.compare(calcScore(entry.Key), entry.Key, next) == 0 {
					next.Entry.Value = entry.Value
				}
				break
			} else {
				p = next
				next = next.next[i]
			}
		}
		prev[i] = p
	}
	return p
}

func (list *SkipList) Add(entry *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	prev := make([]*Node, kMaxHeight)
	p := list.FindGreaterOrEqual(entry, prev)
	if p.next[0] != nil && bytes.Compare(entry.Key, p.next[0].Entry.Key) == 0 {
		return nil
	}
	height := list.randomHeight()
	if height > list.GetMaxHeight() {
		for i := list.GetMaxHeight(); i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}

	p = NewNode(entry, height)
	for i := 0; i < height; i++ {
		next := prev[i].next[i]
		p.next[i] = next
		prev[i].next[i] = p
	}

	return nil
}

func (list *SkipList) Search(key []byte) *Node {
	list.lock.RLock()
	defer list.lock.RUnlock()
	p := list.head
	level := list.GetMaxHeight() - 1
	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			if list.KeyIsAfterNode(key, next) {
				if list.compare(calcScore(key), key, next) == 0 {
					return next
				}
				break
			} else {
				p = next
				next = next.next[i]
			}
		}
	}
	return nil
}

func (list *SkipList) GetMaxHeight() int {
	return list.maxHeight
}

func (list *SkipList) KeyIsAfterNode(key []byte, next *Node) bool {
	if next != nil && list.compare(calcScore(key), key, next) <= 0 {
		return true
	}
	return false
}

func (list *SkipList) compare(score float64, key []byte, next *Node) int {
	if score == next.score {
		return bytes.Compare(key, next.Entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
	return 0
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) randomHeight() int {
	h := 1
	for h < kMaxHeight && list.rand.Intn(2) == 0 {
		h++
	}
	return h
}

func (list *SkipList) PrintSkipList() {
	p := list.head
	level := list.GetMaxHeight() - 1
	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			fmt.Printf("(%s, %s) -> ", next.Entry.Key, next.Entry.Value)
			next = next.next[i]
		}
		fmt.Println()
	}
}
