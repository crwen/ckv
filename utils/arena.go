package utils

import (
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	kBlockSize = 4096

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
func NewArena(sz int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	arena := &Arena{
		//buf: make([]byte, 3*kBlockSize),
		buf: make([]byte, sz+(1<<20)),
		//remaining: kBlockSize,
	}
	return arena
}

func (s *Arena) Allocate(bytes uint32) uint32 {
	offset := atomic.AddUint32(&s.offset, bytes)

	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > kBlockSize {
			growBy = kBlockSize
		}
		if growBy < bytes {
			growBy = bytes
		}
		//buf := make([]byte, growBy)
		//s.buf = append(s.buf, buf...)
		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - bytes
	//if bytes <= s.remaining {
	//	offset := atomic.AddUint32(&s.offset, bytes)
	//	s.remaining -= bytes
	//	return offset - bytes
	//}
	//
	//return s.allocateFallback(bytes)
}

//func (s *Arena) allocateFallback(bytes uint32) uint32 {
//	if bytes > kBlockSize/4 {
//		// to avoid wasting too match space in leftover bytes
//		offset := s.allocateNewBlock(bytes)
//		return offset
//	}
//	s.offset = s.allocateNewBlock(kBlockSize)
//	s.remaining = kBlockSize
//
//	result := s.offset
//	s.offset += bytes
//	s.remaining -= bytes
//	return result
//}

//func (s *Arena) allocateNewBlock(blockBytes uint32) uint32 {
//	buf := make([]byte, blockBytes)
//	s.buf = append(s.buf, buf...)
//	atomic.AddUint64(&s.usage, uint64(blockBytes)+uint64(nodeAlign))
//	return uint32(s.usage - uint64(len(buf)))
//}

func (s *Arena) allocateAligned(blockBytes uint32) {

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

//func (s *Arena) putVal(v ValueStruct) uint32 {
//	l := uint32(v.EncodedSize())
//	offset := s.Allocate(l)
//	v.EncodeValue(s.buf[offset:])
//	return offset
//}

func (s *Arena) putVal(v []byte) uint32 {
	l := uint32(len(v))
	offset := s.Allocate(l)
	//v.EncodeValue(s.buf[offset:])
	buf := s.buf[offset : offset+l]
	AssertTrue(len(v) == copy(buf, v))

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
	if offset == 0 {
		return nil
	}
	return (*Node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getVal(offset uint32) ([]byte, int) {
	//DecodeValue(s.buf[offset : offset+size])
	buf := s.buf[offset:]
	sz := codec.DecodeVarint32(buf)
	valOff := codec.VarintLength(uint64(sz))
	return buf[valOff : valOff+sz], sz
}

func (s *Arena) getKey(offset uint32) ([]byte, int) {
	//DecodeValue(s.buf[offset : offset+size])
	buf := s.buf[offset:]
	sz := codec.DecodeVarint32(buf)
	keyOff := codec.VarintLength(uint64(sz))
	seq := make([]byte, 8)
	codec.DecodeVarint64(seq)
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

// getKey returns byte slice at offset.
//func (s *Arena) getKey(offset uint32, size uint16) []byte {
//	return s.buf[offset : offset+uint32(size)]
//}
//
//// getVal returns byte slice at offset. The given size should be just the value
//// size and should NOT include the meta bytes.
//func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
//	ret.DecodeValue(s.buf[offset : offset+size])
//	return
//}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
