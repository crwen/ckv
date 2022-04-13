package lsm

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
