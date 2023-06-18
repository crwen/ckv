package codec

import (
	"ckv/utils/convert"
	"ckv/utils/errs"
	"encoding/binary"
	"github.com/pkg/errors"
	"hash/crc32"
	"math"
)

// codec
var (
	MagicText    = [...]byte{'S', 'I', 'M', 'P', 'L', 'E', 'K', 'V'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

func EncodeVarint32(buf []byte, v uint32) int {

	var B uint32 = 128
	if v < (1 << 7) {
		buf[0] = byte(v)
		return 1
	} else if v < (1 << 14) {
		buf[0] = byte(v | B)
		buf[1] = byte(v >> 7)
		return 2
	} else if v < (1 << 21) {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[2] = byte(v >> 14)
		return 3
	} else if v < (1 << 28) {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[3] = byte((v >> 14) | B)
		buf[4] = byte(v >> 21)
		return 4
	} else {
		buf[0] = byte(v | B)
		buf[1] = byte((v >> 7) | B)
		buf[3] = byte((v >> 14) | B)
		buf[4] = byte((v >> 21) | B)
		buf[5] = byte(v >> 28)
		return 5
	}
}

func DecodeVarint32(buf []byte) int {
	v, _ := binary.Uvarint(buf)
	v = v & ((1 << 32) - 1)
	return int(v)
}

// VarintLength return the length that needed
// the highest bit is used to mark the end
func VarintLength(v uint64) int {
	len := 1
	for v >= 128 {
		v >>= 7
		len++
	}
	return len
}

func EncodeVarint64(buf []byte, v uint64) int {
	return binary.PutUvarint(buf, v)
}

func DecodeVarint64(buf []byte) uint64 {
	v, _ := binary.Uvarint(buf)
	return v
}

// | length | key |
func EncodeKey(key []byte) []byte {
	keySize := len(key)
	sz := VarintLength(uint64(keySize))
	buf := make([]byte, sz+keySize)
	w := EncodeVarint32(buf, uint32(keySize))
	copy(buf[w:], key)
	w += len(key)
	return buf
}

func DecodeKey(buf []byte) []byte {
	keySize := DecodeVarint32(buf)
	sz := VarintLength(uint64(keySize))
	return buf[sz : sz+keySize]
}

func DecodeTo(buf []byte) []byte {
	keySize := DecodeVarint32(buf)
	sz := VarintLength(uint64(keySize))
	return buf[sz : sz+keySize]
}

//func EncodeFixed64(buf []byte, v uint64) int {
//
//}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := convert.BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(errs.ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

func CalculateU32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, CastagnoliCrcTable)
}

// VerifyU32Checksum crc32
func VerifyU32Checksum(data []byte, expected uint32) error {
	actual := crc32.Checksum(data, CastagnoliCrcTable)
	if actual != expected {
		return errors.Wrapf(errs.ErrChecksumMismatch, "actual: %d, expected: %d", actual, expected)
	}

	return nil
}

// KeyWithTs generates a new key by appending ts to key.
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}

// ParseTs parses the timestamp from the key bytes.
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}

// ParseKey parses the actual key from the key bytes.
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return key
	}

	return key[:len(key)-8]
}
