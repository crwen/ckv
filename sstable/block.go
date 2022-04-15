package sstable

import (
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"unsafe"
)

type Block struct {
	Offset           int
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

func (b *Block) readEntry(buf []byte, sz uint32) (key, value []byte) {
	entryData := buf[:4] // header
	h := &Header{}
	h.decode(entryData)
	overlap := h.Overlap
	diff := h.Diff

	diffKey := buf[4 : 4+diff] // read diff key
	if len(b.BaseKey) == 0 {
		b.BaseKey = diffKey
		key = b.BaseKey
	} else {
		k := make([]byte, overlap+diff)
		copy(k, b.BaseKey[:overlap])
		copy(k[overlap:], diffKey)
		key = k
	}
	value = buf[4+diff : sz]
	return key, value
}

// ReadEntryOffsets return the start Offset of first entry offsets
func (b *Block) readEntryOffsets(buf []byte) uint32 {
	// read checksum and length
	offset := len(buf) - 4
	b.checksumLen = int(convert.BytesToU32(buf[offset:]))
	offset -= b.checksumLen
	b.checksum = buf[offset : offset+b.checksumLen] // read checksum
	if err := codec.VerifyChecksum(buf[:offset], b.checksum); err != nil {
		//return nil, err
	}

	// read entry offsets and length
	offset -= 4
	numEntries := convert.BytesToU32(buf[offset : offset+4])
	offset -= int(numEntries) * 4
	b.EntryOffsets = convert.BytesToU32Slice(buf[offset : offset+int(numEntries)*4])

	// read kv data
	b.Data = buf[:offset]
	return uint32(offset)
	//buf = buf[:offset]

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

func (h Header) encode() []byte {
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
