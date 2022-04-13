package utils

import "hash/crc32"

// codec
var (
	MagicText    = [...]byte{'S', 'I', 'M', 'P', 'L', 'E', 'K', 'V'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
