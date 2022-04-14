package lsm

import "unsafe"

type Block struct {
	offset           int
	checksum         []byte
	entiresIndexStar int
	checksumLen      int

	data []byte
	//restart []uint32

	baseKey      []byte
	entryOffsets []uint32
	end          int
	estimateSz   int64
}

type BlockBuilder struct {
	offset int

	data    []byte
	restart []uint32
	counter int
	lastKey []byte
}

type IndexBlock struct {
	blockOffsets []*BlockOffset
	filter       []byte
	keyCount     uint32
}

type BlockOffset struct {
	Key    []byte
	Offset uint32
	Len    uint32
}

type header struct {
	overlap uint16
	diff    uint16
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}
