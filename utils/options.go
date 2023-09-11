package utils

import "ckv/utils/cmp"

// TODO options
// Options to control the behavior of a database (passed to DB::Open)
type Options struct {
	// ValueThreshold      int64
	Comparable     cmp.Comparator
	WorkDir        string
	MemTableSize   int64 // the threshold to turn memTable to immutable memTable
	SSTableMaxSz   int64 // the threshold to compact
	TableCacheSize int64 // the size of cache to store table
	BlockCacheSize int64 // the size of cache size to store block
	BlockSize      int32 // the size of data block in sst

	// MaxBatchCount       int64
	// MaxBatchSize        int64 // max batch size in bytes
	// ValueLogFileSize    int
	// VerifyValueChecksum bool
	// ValueLogMaxEntries  uint32
	LogRotatesToFlush  int32
	MaxTableSize       int64
	BloomFalsePositive float64
	MaxLevelNum        int // max level of sst
}
