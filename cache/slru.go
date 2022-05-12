package cache

type segmentedLRU struct {
	protectedCap, probationCap   int
	protected, probation         *List
	protectedSize, probationSize int
}

type sNode struct {
	node   *Node
	status int
}

func newSLRU(capacity int) *segmentedLRU {
	probationCap := capacity / 10 * 2
	protectedCap := capacity - probationCap
	return &segmentedLRU{
		probation:    newList(),
		protected:    newList(),
		protectedCap: protectedCap,
		probationCap: probationCap,
	}
}

func (slru *segmentedLRU) add(node *Node) {
	if slru.probationSize == slru.probationCap {
		return
	}
	slru.probation.Put2Head(node)
}

func (slru *segmentedLRU) evict(status int) *Node {
	// evict probation
	if status == PROBATION {
		if slru.probationSize < slru.probationCap {
			return nil
		}
		return slru.probation.Back()
	}
	// evict protected
	if slru.protectedSize < slru.protectedCap {
		return nil
	}
	return slru.protected.Back()
}

func (slru *segmentedLRU) remove(sevict *Node, status int) *Node {
	if status == PROBATION {
		return slru.probation.Remove(sevict)
		slru.probationSize--
	} else {
		return slru.protected.Remove(sevict)
		slru.protectedSize--
	}
	return nil
}

func (slru *segmentedLRU) put2Head(node *Node, to int) {
	if to == PROBATION {
		slru.probation.Put2Head(node)
		slru.probationSize++
	} else {
		slru.protected.Put2Head(node)
		slru.protectedSize++
	}
}

func (slru *segmentedLRU) promote(node *Node) {
	slru.probation.Remove(node)
	slru.probationSize--
	slru.protected.Put2Head(node)
}
