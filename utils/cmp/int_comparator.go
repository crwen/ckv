package cmp

import "bytes"

type IntComparator struct{}

func (cmp IntComparator) Compare(a, b []byte) int {
	sa := calc(a)
	sb := calc(b)
	if sa == sb {
		return bytes.Compare(a, b)
	}
	if sa < sb {
		return -1
	} else {
		return 1
	}
}

func calc(key []byte) int {
	var value int
	l := len(key)
	for i := 0; i < l; i++ {
		value = value*10 + int(key[i]) - 48
	}

	return value
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
