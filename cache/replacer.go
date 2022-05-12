package cache

type Replacer interface {
	Get(key string) interface{}
	Put(key string, value interface{})
}

type Node struct {
	key   string
	value interface{}
	next  *Node
	prev  *Node
}

type List struct {
	head *Node
	tail *Node
	sz   int
}

func newList() *List {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head
	return &List{
		head: head,
		tail: tail,
	}
}

func (list *List) RemoveLast() *Node {
	return list.Remove(list.tail.prev)
}

func (list *List) Remove(node *Node) *Node {
	list.sz--
	prev := node.prev
	next := node.next
	prev.next = next
	next.prev = prev
	node.prev = nil
	node.next = nil
	return node
}

func (list *List) Put2Head(node *Node) {
	list.sz++
	next := list.head.next
	node.next = next
	next.prev = node
	node.prev = list.head
	list.head.next = node
}

func (list *List) move2Head(node *Node) {
	list.Remove(node)
	list.Put2Head(node)
}

func (list *List) InsertAfter(node *Node, insert *Node) {
	list.sz++
	next := node.next
	node.next = insert
	insert.next = next
	next.prev = insert
	insert.prev = node
}

func (list *List) InsertLast(node *Node) {
	list.sz++
	prev := list.tail.prev
	prev.next = node
	node.prev = prev
	node.next = list.tail
	list.tail.prev = node
}

func (list *List) Len() int {
	return list.sz
}

func (list *List) Back() *Node {
	if list.tail.prev == list.head {
		return nil
	}
	return list.tail.prev
}
