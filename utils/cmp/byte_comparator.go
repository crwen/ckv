package cmp

import "bytes"

type ByteComparator struct {
}

func (cmp ByteComparator) Compare(a, b []byte) int {

	return bytes.Compare(a, b)
}
