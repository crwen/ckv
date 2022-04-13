package utils

import (
	"encoding/binary"
	"hash/crc32"
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

//func EncodeFixed64(buf []byte, v uint64) int {
//
//}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
