package sstable

import (
	"SimpleKV/utils"
	"SimpleKV/utils/cmp"
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"io"
	"unsafe"
)

type Block struct {
	Offset            int
	checksum          []byte
	entriesIndexStart int
	checksumLen       int

	Data []byte
	//restart []uint32

	BaseKey      []byte
	EntryOffsets []uint32
	End          int
	EstimateSz   int64
}

func (b *Block) readEntry(buf []byte, sz uint32) (key, value []byte, seq uint64) {
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
	seq = convert.BytesToU64(buf[4+diff : 12+diff])
	value = buf[12+diff : sz]
	return key, value, seq
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
	err          error
	idx          int
	key          []byte
	val          []byte
	it           utils.Item
	cmp          cmp.Comparator
}

func (iter *BlockIterator) setBlock(b *Block, cmp cmp.Comparator) {
	iter.block = b
	iter.err = nil
	iter.idx = -1
	iter.baseKey = iter.baseKey[:0]
	//itr.prevOverlap = 0
	iter.key = iter.key[:0]
	iter.val = iter.val[:0]
	// Drop the index from the block. We don't need it anymore.
	iter.data = b.Data[:b.entriesIndexStart]
	iter.entryOffsets = b.EntryOffsets
	iter.cmp = cmp
}

func (iter *BlockIterator) setIdx(idx int) {
	iter.idx = idx
	if iter.idx >= len(iter.entryOffsets) || iter.idx < 0 {
		iter.err = io.EOF
		return
	}
	var seq uint64
	if iter.idx == len(iter.block.EntryOffsets)-1 {
		iter.key, iter.val, seq = iter.block.readEntry(
			iter.block.Data[iter.block.EntryOffsets[iter.idx]:],
			uint32(iter.block.entriesIndexStart)-iter.block.EntryOffsets[iter.idx])
	} else {
		iter.key, iter.val, seq = iter.block.readEntry(
			iter.block.Data[iter.block.EntryOffsets[iter.idx]:],
			iter.block.EntryOffsets[iter.idx+1]-iter.block.EntryOffsets[iter.idx])
	}
	e := &utils.Entry{
		Key:   iter.key,
		Value: iter.val,
		Seq:   seq,
	}
	iter.it = e
}

func (iter *BlockIterator) Next() {
	iter.setIdx(iter.idx + 1)
}

func (iter *BlockIterator) Valid() bool {
	return iter.err != io.EOF
}

func (iter *BlockIterator) Rewind() {
	iter.setIdx(-1)
}

func (iter *BlockIterator) Item() utils.Item {
	return iter.it
}

func (iter *BlockIterator) Close() error {
	return nil
}
func (itr *BlockIterator) Error() error {
	return itr.err
}

func (iter *BlockIterator) Seek(key []byte) {
	iter.err = nil
	for i := 0; i < len(iter.block.EntryOffsets); i++ {
		iter.setIdx(i)
		if iter.cmp.Compare(iter.key, key) >= 0 {
			break
		}
	}
	//startIndex := 0 // This tells from which index we should start binary search.
	//
	//foundEntryIdx := sort.Search(len(iter.entryOffsets), func(idx int) bool {
	//	// If idx is less than start index then just return false.
	//	if idx < startIndex {
	//		return false
	//	}
	//	iter.setIdx(idx)
	//	return iter.cmp.Compare(iter.key, key) >= 0
	//})
	//
	//iter.setIdx(foundEntryIdx)
}

// seekToFirst brings us to the first element.
func (itr *BlockIterator) seekToFirst() {
	itr.setIdx(0)
}

func (itr *BlockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}
