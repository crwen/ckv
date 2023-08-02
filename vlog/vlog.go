package vlog

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

// VLogFile
type VLogFile struct {
	f    *file.MmapFile
	lock *sync.RWMutex

	opt     *file.Options
	buf     *bytes.Buffer
	writeAt uint32
	size    uint32
}

type VLogHeader struct {
	checksum uint32
	keyLen   uint32
	ValueLen uint32
	types    uint8
}

type VLogRecord struct {
	VLogHeader
	key   []byte
	value []byte
}

// OpenvlogFile _
func OpenVLogFile(opt *file.Options) *VLogFile {
	omf, err := file.OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	if err != nil {
		panic(err)
	}
	wf := &VLogFile{f: omf, lock: &sync.RWMutex{}, opt: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	errs.Err(err)
	return wf
}

// Write
// +---------------------------------------------------+
// | checksum | key len | value len | type | key:value |
// +---------------------------------------------------+
func (vlog *VLogFile) Write(entry *utils.Entry) error {
	vlog.lock.Lock()
	defer vlog.lock.Unlock()

	keyLen := codec.VarintLength(uint64(len(entry.Key)))
	valLen := codec.VarintLength(uint64(len(entry.Value)))

	// checksum + key len + value len + type  + seq(8)
	total := 4 + keyLen + valLen + 1 + len(entry.Key) + len(entry.Value)

	buf := make([]byte, total)
	off := 4
	// write key len , write len and type
	off += codec.EncodeVarint32(buf[off:], uint32(len(entry.Key)))
	off += codec.EncodeVarint32(buf[off:], uint32(len(entry.Value)))

	buf[off] = 1
	off += 1

	vlog.buf.Bytes()
	// write key value
	copy(buf[off:off+len(entry.Key)], entry.Key)
	off += len(entry.Key)

	copy(buf[off:], entry.Value) // write value
	checksum := codec.CalculateU32Checksum(buf[4:])
	copy(buf[:4], convert.U32ToBytes(checksum)) // write checksum

	dst, err := vlog.f.Bytes(int(vlog.writeAt), int(total))
	if err != nil {
		return err
	}
	copy(dst, buf)
	vlog.writeAt += uint32(total)
	return nil
}

func (vlog *VLogFile) ReadAt(pos uint32) ([]byte, error) {
	vlog.lock.RLock()
	defer vlog.lock.RUnlock()
	reader := bufio.NewReader(vlog.f.NewReader(int(pos)))

	record, _, err := vlog.readRecord(reader)
	return record.value, err
}

func (vlog *VLogFile) readRecord(reader *bufio.Reader) (*VLogRecord, int, error) {

	var record = &VLogRecord{}
	checksum := make([]byte, 4)
	if _, err := io.ReadFull(reader, checksum); err != nil {
		return nil, 0, err
	}
	record.checksum = convert.BytesToU32(checksum)

	keySz, err := codec.ReadUVarint32(reader)
	if err != nil {
		return nil, 0, err
	}
	record.keyLen = keySz

	valSz, err := codec.ReadUVarint32(reader)
	record.ValueLen = valSz

	if err != nil {
		return nil, 0, err
	}

	length := codec.VarintLength(uint64(keySz)) + codec.VarintLength(uint64(valSz)) + 1
	buf := make([]byte, uint32(length)+keySz+valSz)
	off := codec.EncodeVarint32(buf, uint32(keySz))
	off += codec.EncodeVarint32(buf[off:], uint32(valSz))

	if types, err := reader.ReadByte(); err != nil {
		return nil, 0, err
	} else {
		buf[off] = types
		off += 1
		record.types = types
	}

	//io.ReadFull(reader, buf)
	if _, err := io.ReadFull(reader, buf[length:]); err != nil {
		return nil, 0, err
	}

	record.key = buf[uint32(length) : uint32(length)+keySz]
	record.value = buf[uint32(length)+keySz:]

	if err := codec.VerifyU32Checksum(buf, record.checksum); err != nil {
		return nil, 0, err
	}

	return record, 4 + len(buf), nil
}

func (vlog *VLogFile) Pos() uint32 {
	return vlog.writeAt
}

func (vlog *VLogFile) Iterate(fn func(e *utils.Entry) error) error {
	vlog.lock.RLock()
	defer vlog.lock.RUnlock()
	reader := bufio.NewReader(vlog.f.NewReader(0))

	var pos uint32 = 0
	for {
		record, n, err := vlog.readRecord(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		pos += uint32(n)
		e := &utils.Entry{
			Key:   record.key,
			Value: record.value,
		}
		fn(e)
	}
}

func (vlog *VLogFile) Fid() uint64 {
	return vlog.opt.FID
}

func (vlog *VLogFile) Close() error {
	if vlog == nil {
		return nil
	}
	if err := vlog.f.Close(); err != nil {
		return err
	}
	return nil
}

func (vlog *VLogFile) Remove() error {
	if vlog == nil {
		return nil
	}
	filename := vlog.f.Fd.Name()
	if err := vlog.f.Close(); err != nil {
		return err
	}
	return os.Remove(filename)
}

func (vlog *VLogFile) Name() string {
	return vlog.f.Fd.Name()
}

func (vlog *VLogFile) Size() uint32 {
	return vlog.writeAt
}
