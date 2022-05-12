package cache

const (
	WINDOW = iota
	PROBATION
	PROTECTED
)

type WinTinyLFU struct {
	data            map[string]*sNode
	winLRU          *List
	slru            *segmentedLRU
	cmSketch        *cmSketch
	winCap, slruCap int
	winSize         int
	w               int
	threshold       int
}

func NewWinTinyLFU(capacity int) *WinTinyLFU {
	slruCap := capacity / 100 * 99
	winCap := capacity - slruCap
	if winCap < 1 {
		winCap = 10
	}
	return &WinTinyLFU{
		data:      make(map[string]*sNode),
		winLRU:    newList(),
		slru:      newSLRU(slruCap),
		cmSketch:  newCmSketch(int64(capacity)),
		winCap:    winCap,
		slruCap:   slruCap,
		threshold: capacity,
	}
}

func (w *WinTinyLFU) Get(key string) interface{} {
	if sNode, ok := w.data[key]; ok {
		w.cmSketch.Increment(keyToHash(key))
		switch sNode.status {
		case WINDOW:
			w.winLRU.move2Head(sNode.node)
		case PROBATION:
			//w.slru.promote(sNode.node)
			w.slru.remove(sNode.node, PROBATION)
			//w.slru.probation.Remove(sNode.node)
			// move to protected
			sNode.status = PROTECTED
			if p2evict := w.slru.evict(PROTECTED); p2evict != nil {
				w.slru.put2Head(sNode.node, PROTECTED)
				w.slru.put2Head(p2evict, PROBATION)
				sp2node := w.data[p2evict.key]
				sp2node.status = PROBATION
			} else {
				w.slru.put2Head(sNode.node, PROTECTED)
			}
		case PROTECTED:
			w.slru.protected.move2Head(sNode.node)
		}
		return sNode.node.value
	}
	return nil

}

func (w *WinTinyLFU) Put(key string, value interface{}) {
	w.w++
	if w.w == w.threshold {
		w.cmSketch.Reset()
		w.w = 0
	}
	if sNode, ok := w.data[key]; ok {
		switch sNode.status {
		case WINDOW:
			w.winLRU.move2Head(sNode.node)
		case PROBATION:
			w.slru.probation.move2Head(sNode.node)
		case PROTECTED:
			w.slru.protected.move2Head(sNode.node)
		}
		return
	}
	newSNode := &sNode{node: &Node{key: key, value: value}}
	var wevict *sNode
	if w.winSize == w.winCap {
		eNode := w.winLRU.RemoveLast()
		wevict = &sNode{node: eNode}
		w.winSize--
	}
	newSNode.status = WINDOW
	w.winLRU.Put2Head(newSNode.node)
	w.data[key] = newSNode
	w.winSize++

	if wevict != nil {
		if sevict := w.slru.evict(PROBATION); sevict != nil {
			if w.win(wevict.node, sevict) {
				wevict.status = PROBATION
				w.slru.add(wevict.node)

				w.data[wevict.node.key] = wevict
				w.slru.remove(sevict, PROBATION)
				delete(w.data, sevict.key)
			} else {
				delete(w.data, wevict.node.key)
			}
		} else {
			// there are free space to hold
			wevict.status = PROBATION
			w.slru.add(wevict.node)
		}
	}
}

func (w *WinTinyLFU) win(evict1, evict2 *Node) bool {
	if w.cmSketch.Estimate(keyToHash(evict1.key)) > w.cmSketch.Estimate(keyToHash(evict2.key)) {
		return true
	}
	return false
}
