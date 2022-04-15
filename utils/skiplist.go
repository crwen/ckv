package utils

import (
	"SimpleKV/utils/cmp"
	"SimpleKV/utils/codec"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync"
)

const (
	kMaxHeight = 12
)

var (
	defaultComparator cmp.Comparator = cmp.ByteComparator{}
)

type SkipList struct {
	head       *Node
	maxHeight  int
	rand       *rand.Rand
	arena      *Arena
	comparator cmp.Comparator
	lock       sync.RWMutex
}

type Node struct {
	//Entry     *Entry
	keyOffset   uint32
	valueOffset uint32
	next        [kMaxHeight]*Node
}

type Key struct {
	keyOffset uint32
	keySize   uint32
}

type Value struct {
	valueOffset uint32
	valueSize   uint32
}

func NewNode(arena *Arena, entry *Entry, height int) *Node {
	keySize := len(entry.Key)
	valSize := len(entry.Value)
	//internalKeySize := keySize + 8
	internalKeySize := keySize
	encodedLen := codec.VarintLength(uint64(internalKeySize)) +
		internalKeySize + codec.VarintLength(uint64(valSize)) + valSize

	offset := arena.Allocate(uint32(encodedLen))
	kw := arena.PutKey(entry.Key, offset)
	arena.PutVal(entry.Value, offset+kw)

	nodeOffset := arena.putNode(height)

	node := arena.getNode(nodeOffset)
	//node.key = &Key{keyOffset: offset, keySize: uint32(keySize)}
	node.keyOffset = offset
	node.valueOffset = offset + kw
	//node.value = &Value{valueOffset: offset + kw, valueSize: uint32(valSize)}
	//node.next = make([]*Node, height)
	return node

}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func (node *Node) Next(height int) *Node {
	return node.next[height]
}

func (node *Node) getKey(arena *Arena) []byte {
	k, _ := arena.getKey(node.keyOffset)
	return k
}

func (node *Node) getValue(arena *Arena) []byte {
	v, _ := arena.getVal(node.valueOffset)
	return v
}

func NewSkipList(arena *Arena) *SkipList {
	list := &SkipList{
		head:       NewNode(arena, &Entry{Key: []byte{0}}, kMaxHeight),
		maxHeight:  0,
		rand:       r,
		arena:      arena,
		comparator: defaultComparator,
		lock:       sync.RWMutex{},
	}
	return list
}

func NewSkipListWithComparator(arena *Arena, comparator cmp.Comparator) *SkipList {
	list := &SkipList{
		head:       NewNode(arena, &Entry{Key: []byte{0}}, kMaxHeight),
		maxHeight:  0,
		rand:       r,
		arena:      arena,
		comparator: comparator,
		lock:       sync.RWMutex{},
	}
	return list
}

func (list *SkipList) Close() {
	list.arena = nil
}

func (list *SkipList) FindGreaterOrEqual(key []byte, prev []*Node) *Node {
	p := list.head
	level := list.GetMaxHeight() - 1

	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			if list.KeyIsAfterNode(key, next) {
				//if prev != nil && list.compare(calcScore(entry.Key), entry.Key, next) == 0 {
				//	next.Entry.Value = entry.Value
				//}
				break
			} else {
				p = next
				next = next.next[i]
			}
		}
		if prev != nil {
			prev[i] = p
		}
	}
	return p
}

func (list *SkipList) Add(entry *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	prev := make([]*Node, kMaxHeight)
	p := list.FindGreaterOrEqual(entry.Key, prev)
	//if p.next[0] != nil && bytes.Compare(entry.Key, p.next[0].Entry.Key) == 0 {
	//	return nil
	//}
	height := list.randomHeight()
	if height > list.GetMaxHeight() {
		for i := list.GetMaxHeight(); i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}

	p = NewNode(list.arena, entry, height)
	for i := 0; i < height; i++ {
		next := prev[i].next[i]
		p.next[i] = next
		prev[i].next[i] = p
	}

	return nil
}

func (list *SkipList) Search(key []byte) []byte {
	list.lock.RLock()
	defer list.lock.RUnlock()
	p := list.head
	level := list.GetMaxHeight() - 1
	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			if list.KeyIsAfterNode(key, next) {
				if i == 0 && list.comparator.Compare(key, next.getKey(list.arena)) == 0 {
					return next.getValue(list.arena)
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
	if next != nil && list.comparator.Compare(key, next.getKey(list.arena)) <= 0 {
		return true
	}
	return false
}

func (list *SkipList) randomHeight() int {
	h := 1
	for h < kMaxHeight && list.rand.Intn(2) == 0 {
		h++
	}
	return h
}

func (s *SkipList) Size() int64 { return s.arena.size() }

func (list *SkipList) PrintSkipList() {
	p := list.head
	level := list.GetMaxHeight() - 1
	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {

			fmt.Printf("(%s, %s) -> ", next.getKey(list.arena), next.getValue(list.arena))
			next = next.next[i]
		}
		fmt.Println()
	}
}

type SkipListIterator struct {
	list *SkipList
	node *Node
}

func (list *SkipList) NewIterator() *SkipListIterator {
	return &SkipListIterator{
		list: list,
		node: list.head,
	}
}

func (iter *SkipListIterator) Next() {
	AssertTrue(iter.Valid())
	iter.node = iter.node.next[0]
}

func (iter *SkipListIterator) Valid() bool {
	return iter.node != nil
}

func (iter *SkipListIterator) Rewind() {
	iter.node = iter.list.head.next[0]
}

func (iter *SkipListIterator) Key() []byte {
	return iter.node.getKey(iter.list.arena)
}

func (iter *SkipListIterator) Value() []byte {
	return iter.node.getValue(iter.list.arena)
}

func (iter *SkipListIterator) Item() Item {
	if !iter.Valid() {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}

	return &Entry{
		Key:   iter.node.getKey(iter.list.arena),
		Value: iter.node.getValue(iter.list.arena),
	}
}

func (iter *SkipListIterator) Close() error {
	iter.list.Close()
	return nil
}

func (iter *SkipListIterator) Seek(key []byte) {
	iter.Rewind()
	iter.list.FindGreaterOrEqual(key, nil)
	//iter.Next()
	//for n := iter.Item(); n != nil && bytes.Compare(n.Entry().Key, key) != 0; {
	//	n = iter.Item()
	//	iter.Next()
	//}
}
