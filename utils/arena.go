package utils

import (
	"ckv/utils/codec"
	"ckv/utils/convert"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	kBlockSize uint32 = 4096

	offsetSize = int(unsafe.Sizeof(uint32(0)))

	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(Node{}))
	nodeSize    = int(unsafe.Sizeof(Node{}))
	nodePtrSize = int(unsafe.Sizeof(&Node{}))
)

type Arena struct {
	buf []byte
	//remaining uint32 // 剩余可用内存
	offset uint32
	//usage     uint64 // 已经分配的总量
	//shouldGrow bool
}

// newArena returns a new arena.
func NewArena() *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	arena := &Arena{
		buf: make([]byte, 3*kBlockSize),
		//buf: make([]byte, kBlockSize),
		//remaining: kBlockSize,
	}
	return arena
}

func (s *Arena) Allocate(bytes uint32) uint32 {
	offset := atomic.AddUint32(&s.offset, bytes)

	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := kBlockSize

		if growBy < bytes {
			growBy = (bytes/kBlockSize + 1) * kBlockSize
		}
		//buf := make([]byte, growBy)
		//s.buf = append(s.buf, buf...)
		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - bytes
}

func (s *Arena) size() int64 {
	//return int64(atomic.LoadUint64(&s.usage))
	return int64(atomic.LoadUint32(&s.offset))
}

// ------------------------------------------
// | key length | key | value length | value|
// ------------------------------------------
func (s *Arena) putNode(height int) uint32 {
	//l := nodeSize + (height-1)*nodePtrSize + nodeAlign
	unusedSize := (kMaxHeight - height) * nodePtrSize
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.Allocate(uint32(l))

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (s *Arena) putVal(v []byte) uint32 {
	l := uint32(len(v))
	offset := s.Allocate(l)
	//v.EncodeValue(s.buf[offset:])
	buf := s.buf[offset : offset+l]
	AssertTrue(len(v) == copy(buf, v))

	return offset
}

func (s *Arena) putData(data []byte) uint32 {
	dataSz := len(data)
	sz := codec.VarintLength(uint64(dataSz)) + dataSz
	offset := s.Allocate(uint32(sz))
	buf := s.buf[offset:]
	w := codec.EncodeVarint32(buf, uint32(dataSz))
	AssertTrue(len(data) == copy(buf[w:], data))
	w += len(data)
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.Allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) PutKey(key []byte, offset uint32) uint32 {
	keySize := len(key)
	buf := s.buf[offset:]
	w := codec.EncodeVarint32(buf, uint32(keySize))
	AssertTrue(len(key) == copy(buf[w:], key))
	w += len(key)
	//w += codec.EncodeVarint64(buf[w:], uint64(time.Now().Unix()))
	return uint32(w)
}

func (s *Arena) PutSeq(seq uint64, offset uint32) uint32 {
	buf := s.buf[offset:]
	w := copy(buf, convert.U64ToBytes(seq))
	//w := codec.EncodeVarint64(buf[:], seq)
	return uint32(w)
}

func (s *Arena) PutVal(val []byte, offset uint32) uint32 {
	valSize := len(val)
	buf := s.buf[offset:]
	w := codec.EncodeVarint32(buf, uint32(valSize))
	AssertTrue(len(val) == copy(buf[w:], val))
	w += len(val)
	return uint32(w)
}

func (s *Arena) getNode(offset uint32) *Node {
	//if offset == 0 {
	//	return nil
	//}
	return (*Node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getVal(offset uint32) ([]byte, int) {
	//DecodeValue(s.buf[offset : offset+size])
	buf := s.buf[offset:]
	sz := codec.DecodeVarint32(buf)
	valOff := codec.VarintLength(uint64(sz))
	return buf[valOff : valOff+sz], sz
}

func (s *Arena) getData(offset uint32) ([]byte, int) {
	buf := s.buf[offset:]
	sz := codec.DecodeVarint32(buf)
	keyOff := codec.VarintLength(uint64(sz))
	return buf[keyOff : keyOff+sz], sz
}

func (s *Arena) getKey(offset uint32) ([]byte, int) {
	buf := s.buf[offset:]
	sz := codec.DecodeVarint32(buf)
	keyOff := codec.VarintLength(uint64(sz))
	return buf[keyOff : keyOff+sz], sz + 8
}

func (s *Arena) getSeq(offset uint32) uint64 {
	buf := s.buf[offset : offset+8]
	//return codec.DecodeVarint64(buf[:])
	return convert.BytesToU64(buf)
}

func (s *Arena) GetKey(offset uint32) ([]byte, int) {
	//DecodeValue(s.buf[offset : offset+size])
	return s.getKey(offset)
}

func (s *Arena) GetVal(offset uint32) ([]byte, int) {
	//DecodeValue(s.buf[offset : offset+size])
	return s.getVal(offset)
}
func (s *Arena) GetSeq(offset uint32) uint64 {
	buf := s.buf[offset:]
	return codec.DecodeVarint64(buf[:])
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
