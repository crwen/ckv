package sstable

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	"SimpleKV/utils/codec"
	"SimpleKV/utils/convert"
	"SimpleKV/utils/errs"
	files "SimpleKV/utils/file"
	"errors"
	"fmt"
	"math"
	"os"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *Block
	opt           *utils.Options
	blockList     []*Block
	index         *IndexBlock
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type buildData struct {
	blockList []*Block
	index     []byte
	checksum  []byte
	size      int
}

func NewTableBuiler(opt *utils.Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

func newTableBuilerWithSSTSize(opt *utils.Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func (tb *tableBuilder) Add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := e.Value
	// 检查是否需要分配一个新的 Block
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		// Create a new Block and start writing.
		tb.curBlock = &Block{
			Data: make([]byte, tb.opt.BlockSize),
		}
	}

	// Append kv data
	// +---------------------------+
	// | header | diff key | value |
	// +---------------------------+

	var differKey []byte
	if len(tb.curBlock.BaseKey) == 0 {
		tb.curBlock.BaseKey = append(tb.curBlock.BaseKey[:0], key...)
		differKey = key
	} else {
		differKey = tb.keyDiff(key)
	}

	h := Header{
		Overlap: uint16(len(key) - len(differKey)),
		Diff:    uint16(len(differKey)),
	}

	tb.curBlock.EntryOffsets = append(tb.curBlock.EntryOffsets, uint32(tb.curBlock.End))

	tb.append(h.encode())
	tb.append(differKey)

	dst := tb.allocate(len(val))
	copy(dst, val)
}

// flush flush data to sst file.
func (tb *tableBuilder) Flush(tableName string) (t *Table, err error) {
	bd := tb.done()
	t = newTable(tb.opt, files.FID(tableName))

	t.ss = OpenSStable(&file.Options{
		FileName: tableName,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})
	t.ss.SetIndex(tb.index)
	t.ss.SetMin(tb.blockList[0].BaseKey)
	buf := make([]byte, bd.size)

	// copy data that needed
	written := bd.Copy(buf)
	errs.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))

	// write to file
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	t.MinKey = tb.blockList[0].BaseKey
	if err = t.ss.Close(); err != nil {
		return t, err
	}
	return t, nil
}

// Copy write data of sst to dst. e.g. data blocks, index, checksum
func (bd *buildData) Copy(dst []byte) int {
	var written int
	// copy data blocks
	for _, blk := range bd.blockList {
		written += copy(dst[written:], blk.Data[:blk.End])
	}
	// copy index and length
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], convert.U32ToBytes(uint32(len(bd.index))))

	// copy checksum and length
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], convert.U32ToBytes(uint32(len(bd.checksum))))

	return written
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	errs.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.BaseKey); i++ {
		if newKey[i] != tb.curBlock.BaseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.EntryOffsets) <= 0 {
		return false
	}

	errs.CondPanic(!((uint32(len(tb.curBlock.EntryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))

	entryOffSz := int64((len(tb.curBlock.EntryOffsets) + 1)) * 4
	entriesOffsetsSize := entryOffSz + // entry offsets list
		4 + // size of list
		8 + // Sum64 in checksum proto
		4 // checksum length
	kvSize := int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(len(e.Value))
	tb.curBlock.EstimateSz = int64(tb.curBlock.End) + kvSize + entriesOffsetsSize

	errs.CondPanic(!(uint64(tb.curBlock.End)+uint64(tb.curBlock.EstimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.EstimateSz > int64(tb.opt.BlockSize)
}

// finishBlock write other info to Block, e.g. entry offsets, checksum
//  +------------------------ --------------------------------------+
//  |  kv_data | entryOffsets | entryOff len | checksum | check len |
//  +---------------------------------------------------------------+
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.EntryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(convert.U32SliceToBytes(tb.curBlock.EntryOffsets))
	tb.append(convert.U32ToBytes(uint32(len(tb.curBlock.EntryOffsets))))

	// Append the Block checksum and its length.
	checksum := tb.calculateChecksum(tb.curBlock.Data[:tb.curBlock.End])
	tb.append(checksum)
	tb.append(convert.U32ToBytes(uint32(len(checksum))))

	tb.estimateSz += tb.curBlock.EstimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.EntryOffsets))
	tb.curBlock = nil // 表示当前block 已经被序列化到内存
	return
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.Data[bb.End:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.Data)
		if bb.End+need > sz {
			sz = bb.End + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.Data)
		bb.Data = tmp
	}
	bb.End += need
	return bb.Data[bb.End-need : bb.End]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := codec.CalculateChecksum(data)
	return convert.U64ToBytes(checkSum)
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	bd := buildData{blockList: tb.blockList}

	// TODO
	// create bloom filter if needed
	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	// TODO 构建索引
	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum

	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4

	return bd
}

func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	index := &IndexBlock{
		BlockOffsets: make([]*BlockOffset, len(tb.blockList)),
		Filter:       nil,
		KeyCount:     tb.keyCount,
	}
	var indexSize int
	if len(bloom) > 0 {
		index.Filter = bloom
		indexSize += len(bloom)
	}
	var offset uint32
	var dataSize uint32
	for i, blk := range tb.blockList {
		index.BlockOffsets[i] = &BlockOffset{
			Key:    blk.BaseKey,
			Offset: offset,
			Len:    uint32(blk.End),
		}
		indexSize += len(blk.BaseKey) + 4 + 4
		offset += uint32(blk.End)
		dataSize += uint32(blk.End)
	}
	index.KeyCount = tb.keyCount
	indexSize += 4

	tb.index = index
	return tb.finishIndexBlock(index, indexSize), dataSize
}

func (tb *tableBuilder) finishIndexBlock(index *IndexBlock, size int) []byte {

	buf := make([]byte, size)
	// Append the block offsets
	off := 0
	offsets := index.BlockOffsets
	for i := range offsets {
		off += copy(buf[off:], convert.U32ToBytes(offsets[i].Offset))
		off += copy(buf[off:], offsets[i].Key)
		off += copy(buf[off:], convert.U32ToBytes(offsets[i].Len))
	}

	// Append the bloom filter
	off += copy(buf[off:], index.Filter)

	// Append the max version
	off += copy(buf[off:], convert.U64ToBytes(tb.maxVersion))
	// Append key count
	off += copy(buf[off:], convert.U32ToBytes(tb.keyCount))

	return buf
}
