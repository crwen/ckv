package sstable

type IndexBlock struct {
	BlockOffsets []*BlockOffset
	Filter       []byte
	KeyCount     uint32
}

type BlockOffset struct {
	Key    []byte
	Offset uint32
	Len    uint32
}
