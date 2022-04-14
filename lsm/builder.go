package lsm

import (
	"SimpleKV/file"
	"SimpleKV/utils"
	files "SimpleKV/utils/file"
	"errors"
	"fmt"
	"math"
	"os"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *Block
	opt           *Options
	blockList     []*Block
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

func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
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
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	// Append kv data
	// +---------------------------+
	// | header | diff key | value |
	// +---------------------------+

	var differKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		differKey = key
	} else {
		differKey = tb.keyDiff(key)
	}

	h := header{
		overlap: uint16(len(key) - len(differKey)),
		diff:    uint16(len(differKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(differKey)

	dst := tb.allocate(len(val))
	copy(dst, val)
}

// flush flush data to sst file.
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{
		ss:  nil,
		lm:  lm,
		fid: files.FID(tableName),
	}
	t.ss = file.NewSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})

	buf := make([]byte, bd.size)

	// copy data that needed
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))

	// write to file
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

// Copy write data of sst to dst. e.g. data blocks, index, checksum
func (bd *buildData) Copy(dst []byte) int {
	var written int
	// copy data blocks
	for _, blk := range bd.blockList {
		written += copy(dst[written:], blk.data[:blk.end])
	}
	// copy index and length
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))

	// copy checksum and length
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))

	return written
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}

	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))

	entryOffSz := int64((len(tb.curBlock.entryOffsets) + 1)) * 4
	entriesOffsetsSize := entryOffSz +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4 // checksum length
	kvSize := int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(len(e.Value))
	tb.curBlock.estimateSz = kvSize + entriesOffsetsSize

	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// finishBlock write other info to Block, e.g. entry offsets, checksum
//  +------------------------ --------------------------------------+
//  |  kv_data | entryOffsets | entryOff len | checksum | check len |
//  +---------------------------------------------------------------+
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	// Append the Block checksum and its length.
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // 表示当前block 已经被序列化到内存
	return
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	bd := buildData{blockList: tb.blockList}

	// TODO
	// create bloom filter if needed
	//var f utils.Filter
	//if tb.opt.BloomFalsePositive > 0 {
	//	bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
	//	f = utils.NewFilter(tb.keyHashes, bits)
	//}

	// TODO 构建索引
	//index, dataSize := tb.buildIndex(f)
	//checksum := tb.calculateChecksum(index)
	sz := 0
	for _, blk := range tb.blockList {
		sz += int(blk.estimateSz)
	}
	bd.size = sz
	return bd
}
