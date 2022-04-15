package sstable

import (
	"unsafe"
)

type Block struct {
	offset           int
	checksum         []byte
	entiresIndexStar int
	checksumLen      int

	Data []byte
	//restart []uint32

	BaseKey      []byte
	EntryOffsets []uint32
	End          int
	EstimateSz   int64
}

//type BlockBuilder struct {
//	offset int
//
//	data    []byte
//	restart []uint32
//	counter int
//	lastKey []byte
//}

type Header struct {
	Overlap uint16
	Diff    uint16
}

const headerSize = uint16(unsafe.Sizeof(Header{}))

func (h Header) Encode() []byte {
	var b [4]byte
	*(*Header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *Header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

type BlockIterator struct {
	block        *Block
	data         []byte
	baseKey      []byte
	entryOffsets []uint32
	end          int
	estimateSz   int64
}

//func (iter *BlockIterator) Next() {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (iter *BlockIterator) Valid() bool {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (iter *BlockIterator) Rewind() {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (iter *BlockIterator) Item() utils.Item {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (iter *BlockIterator) Close() error {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (iter *BlockIterator) Seek(key []byte) {
//	//TODO implement me
//	panic("implement me")
//}
