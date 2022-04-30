package utils

import (
	"SimpleKV/utils/cmp"
	"SimpleKV/utils/codec"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
)

const (
	kMaxHeight = 12
)

var (
	defaultComparator cmp.Comparator = cmp.ByteComparator{}
	seq               uint64
)

type SkipList struct {
	head       *Node
	maxHeight  int
	ref        int32
	rand       *rand.Rand
	arena      *Arena
	comparator cmp.Comparator
	lock       sync.RWMutex
}

type Node struct {
	//Entry     *Entry
	keyOffset   uint32
	seq         uint64
	valueOffset uint32
	next        [kMaxHeight]*Node
}

// IncrRef increase the ref by 1
func (list *SkipList) IncrRef() {
	atomic.AddInt32(&list.ref, 1)
}

// DecrRef decrease the ref by 1. If the ref is 0, close the skip list
func (list *SkipList) DecrRef() {
	newRef := atomic.AddInt32(&list.ref, -1)
	if newRef <= 0 {
		list.arena = nil
	}
}

func NewNode(arena *Arena, entry *Entry, height int) *Node {
	keySize := len(entry.Key)
	valSize := len(entry.Value)
	internalKeySize := keySize + 8
	//internalKeySize := keySize
	encodedLen := codec.VarintLength(uint64(internalKeySize)) +
		internalKeySize + codec.VarintLength(uint64(valSize)) + valSize

	offset := arena.Allocate(uint32(encodedLen))
	kw := arena.PutKey(entry.Key, offset)
	//sequence := time.Now().UnixMilli()
	//sequence := atomic.AddUint64(&seq, 1)
	sw := arena.PutSeq(uint64(entry.Seq), offset+kw)
	arena.PutVal(entry.Value, offset+sw+kw)
	//arena.PutVal(entry.Value, offset+kw)

	nodeOffset := arena.putNode(height)

	node := arena.getNode(nodeOffset)
	//node.key = &Key{keyOffset: offset, keySize: uint32(keySize)}
	node.keyOffset = offset
	node.seq = uint64(entry.Seq)
	node.valueOffset = offset + sw + kw
	//node.valueOffset = offset + kw
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
func (node *Node) getSeq(arena *Arena) uint64 {
	seq := arena.getSeq(node.valueOffset - 8)
	return seq
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
	//list.arena = nil
	list.DecrRef()
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

//func (list *SkipList) Search(key []byte) []byte {
func (list *SkipList) Search(key []byte) *Entry {
	list.lock.RLock()
	defer list.lock.RUnlock()
	p := list.head
	level := list.GetMaxHeight() - 1
	for i := level; i >= 0; i-- {
		for next := p.next[i]; next != nil; {
			if list.KeyIsAfterNode(key, next) {
				if i == 0 && list.comparator.Compare(key, next.getKey(list.arena)) == 0 {
					e := &Entry{Key: key}
					e.Value = next.getValue(list.arena)
					e.Seq = next.getSeq(list.arena)
					//e.Seq
					return e
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

			fmt.Printf("(%s, %s, %d) -> ", next.getKey(list.arena), next.getValue(list.arena), next.getSeq(list.arena))
			//fmt.Printf("(%s, %s, %d) -> ", next.getKey(list.arena), next.getValue(list.arena), next.seq)
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
	// increase ref first
	//list.IncrRef()
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
		Seq:   iter.node.getSeq(iter.list.arena),
	}
}

func (iter *SkipListIterator) Close() error {
	// decrease the ref of skip list
	//iter.list.DecrRef()
	//iter.list.Close()
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
