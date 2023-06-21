package lsm

import (
	"bufio"
	"bytes"
	"ckv/file"
	"ckv/utils"
	"ckv/utils/codec"
	"ckv/utils/convert"
	"ckv/utils/errs"
	"io"
	"os"
	"sync"
)

const walFileExt string = ".wal"

// Wal
type WalFile struct {
	f    *file.MmapFile
	lock *sync.RWMutex

	opt     *file.Options
	buf     *bytes.Buffer
	writeAt uint32
	size    uint32
}

type WalHeader struct {
	checksum uint32
	keyLen   uint16
	ValueLen uint16
	types    uint8
}

// OpenWalFile _
func OpenWalFile(opt *file.Options) *WalFile {
	omf, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		panic(err)
	}
	wf := &WalFile{f: omf, lock: &sync.RWMutex{}, opt: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	errs.Err(err)
	return wf
}

// Write
// +---------------------------------------------------+
// | checksum | key len | value len | type | key:value |
// +---------------------------------------------------+
func (wal *WalFile) Write(entry *utils.Entry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	h := WalHeader{}
	h.keyLen = uint16(len(entry.Key))
	h.ValueLen = uint16(len(entry.Value))
	// checksum + key len + value len + type = 4 + 2 + 2 + 1 = 9
	total := h.keyLen + h.ValueLen + 9 + 8

	buf := make([]byte, total)
	// write key len , write len and type
	copy(buf[4:6], convert.U16ToBytes(h.keyLen))
	copy(buf[6:8], convert.U16ToBytes(h.ValueLen))
	buf[8] = 1
	wal.buf.Bytes()
	// write key value
	copy(buf[9:9+len(entry.Key)], entry.Key)
	pos := 9 + len(entry.Key)
	copy(buf[pos:pos+8], convert.U64ToBytes(entry.Seq))
	copy(buf[pos+8:], entry.Value) // write value
	h.checksum = codec.CalculateU32Checksum(buf[4:])
	copy(buf[:4], convert.U32ToBytes(h.checksum)) // write checksum

	dst, err := wal.f.Bytes(int(wal.writeAt), int(total))
	if err != nil {
		return err
	}
	copy(dst, buf)
	wal.writeAt += uint32(total)
	return nil
}

func (wal *WalFile) Iterate(fn func(e *utils.Entry) error) (uint64, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	reader := bufio.NewReader(wal.f.NewReader(int(0)))

	//data := wal.f.Data
	//data, err := io.ReadAll(reader)
	//if err != nil {
	//	errs.Panic(err)
	//}
	var maxSeq uint64
	for {
		buf := make([]byte, 9)
		if _, err := io.ReadFull(reader, buf); err != nil {
			break
		}
		h := WalHeader{}
		h.checksum = convert.BytesToU32(buf[0:4])
		h.keyLen = convert.BytesToU16(buf[4:6])
		h.ValueLen = convert.BytesToU16(buf[6:8])
		h.types = buf[8]
		b := make([]byte, h.keyLen+h.ValueLen+8)

		//io.ReadFull(reader, buf)
		if _, err := io.ReadFull(reader, b); err != nil {
			break
		}
		//total := 9 + h.keyLen + h.ValueLen
		//key := data[9 : 9+h.keyLen]

		key := b[:h.keyLen]
		seq := convert.BytesToU64(b[h.keyLen : h.keyLen+8])
		//value := data[9+h.keyLen : total]
		value := b[h.keyLen+8 : h.keyLen+8+h.ValueLen]
		//data = data[total:]
		buf = append(buf, b...)
		if err := codec.VerifyU32Checksum(buf[4:], h.checksum); err != nil {
			break
		}

		err := fn(&utils.Entry{Key: key, Value: value, Seq: seq})
		if err != nil {
			break
		}
		maxSeq = seq
		//fmt.Println(string(key), string(value))
	}

	return maxSeq, nil
}

func (wal *WalFile) Fid() uint64 {
	return wal.opt.FID
}

func (wal *WalFile) Close() error {
	if wal == nil {
		return nil
	}
	filename := wal.f.Fd.Name()
	if err := wal.f.Close(); err != nil {
		return err
	}
	return os.Remove(filename)
}

func (wal *WalFile) Name() string {
	return wal.f.Fd.Name()
}

func (wal *WalFile) Size() uint32 {
	return wal.writeAt
}
