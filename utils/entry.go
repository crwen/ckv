package utils

import (
	"ckv/utils/convert"
	"encoding/binary"
	"time"
)

type ValueStruct struct {
	Meta      byte
	Value     []byte
	Seq       uint64
	ExpiresAt uint64
}

func EncodeValue(vs *ValueStruct) []byte {
	valSz := len(vs.Value)
	sz := valSz + 8
	buf := make([]byte, sz, sz)
	copy(buf, convert.U64ToBytes(vs.Seq))
	copy(buf, vs.Value)
	return buf[0:sz]
}

func DecodeValue(value []byte) *ValueStruct {
	v := &ValueStruct{}
	v.Seq = convert.BytesToU64(value[:8])
	v.Value = value[8:]
	return v
}

// value只持久化具体的value值和过期时间
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue
func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

//对value进行编码，并将编码后的字节写入byte
//这里将过期时间和value的值一起编码
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

//Entry _ 最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	ExpiresAt uint64

	KeySize uint32

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

// NewEntry_
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// Entry_
func (e *Entry) Entry() *Entry {
	return e
}

// WithTTL _
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// EncodedSize is the size of the ValueStruct when encoded
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize
func (e *Entry) EstimateSize(threshold int) int {
	// TODO: 是否考虑 user meta?
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 // Meta
	}
	return len(e.Key) + 12 + 1 // 12 for ValuePointer, 2 for meta.
}

// header 对象
// header is used in value log as a header before Entry.
type Header struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode decodes the given header from the provided byte slice.
// Returns the number of bytes read.
func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}
