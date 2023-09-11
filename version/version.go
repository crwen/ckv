package version

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"ckv/utils"
)

const (
	L0_CompactionTrigger = 5
)

type Version struct {
	opt *utils.Options
	f   *os.File
	vf  *os.File

	// refs int
	vset *VersionSet
	next *Version
	prev *Version
	// files []map[uint64]*FileMetaData
	files  [][]*FileMetaData
	vfiles [][]*VFileGroupMetaData
	sync.RWMutex
}

func NewVersion(opt *utils.Options) *Version {
	// files := make([]map[uint64]*FileMetaData, opt.MaxLevelNum)
	files := make([][]*FileMetaData, opt.MaxLevelNum)
	vfiles := make([][]*VFileGroupMetaData, opt.MaxLevelNum)
	for i := 0; i < opt.MaxLevelNum; i++ {
		// files[i] = map[uint64]*FileMetaData{}
		files[i] = make([]*FileMetaData, 0)
		vfiles[i] = make([]*VFileGroupMetaData, 0)
	}
	return &Version{
		opt:     opt,
		files:   files,
		vfiles:  vfiles,
		RWMutex: sync.RWMutex{},
	}
}

func (v *Version) log(level int, fileMetaData *FileMetaData, op byte) {
	buf := make([]byte, 11)
	// binary.BigEndian.PutUint16(buf[0:1], op)
	buf[0] = op
	binary.BigEndian.PutUint16(buf[1:3], uint16(level))
	binary.BigEndian.PutUint64(buf[3:11], fileMetaData.id)
	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
	ssz := len(fileMetaData.smallest)
	lsz := len(fileMetaData.largest)
	buf = make([]byte, ssz+lsz+8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(ssz))
	copy(buf[4:4+ssz], fileMetaData.smallest)
	binary.BigEndian.PutUint32(buf[4+ssz:8+ssz], uint32(lsz))
	copy(buf[8+ssz:8+ssz+lsz], fileMetaData.largest)
	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
}

func (v *Version) readLog() {
	magic := []byte(VersionEdit_BEGIN_MAGIC)
	buf := make([]byte, 2+len(magic))
	binary.BigEndian.PutUint16(buf[0:2], VersionEdit_BEGIN)
	copy(buf[2:], magic)
	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
}

func (v *Version) checkBeginLog(r io.Reader) error {
	magic := []byte(VersionEdit_BEGIN_MAGIC)
	buf := make([]byte, len(magic))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	if !bytes.Equal(buf, magic) {
		return io.EOF
	}
	return nil
}

func (v *Version) checkEndLog(r io.Reader) error {
	magic := []byte(VersionEdit_END_MAGIC)
	buf := make([]byte, len(magic))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	if !bytes.Equal(buf, magic) {
		return io.EOF
	}
	return nil
}

func (v *Version) logBegin() {
	magic := []byte(VersionEdit_BEGIN_MAGIC)
	buf := make([]byte, 1+len(magic))
	buf[0] = VersionEdit_BEGIN
	// binary.BigEndian.PutUint16(buf[0:2], VersionEdit_BEGIN)
	copy(buf[1:], magic)
	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
}

func (v *Version) logEnd() {
	magic := []byte(VersionEdit_END_MAGIC)
	buf := make([]byte, 1+len(magic))
	buf[0] = VersionEdit_END
	// binary.BigEndian.PutUint16(buf[0:2], VersionEdit_BEGIN)
	copy(buf[1:], magic)
	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
}

func (v *Version) vlog(level int, fileMetaData *FileMetaData, op uint16) {
	// | op | level | fid |
	buf := make([]byte, 12)
	binary.BigEndian.PutUint16(buf[0:2], op)
	binary.BigEndian.PutUint16(buf[2:4], uint16(level))
	binary.BigEndian.PutUint64(buf[4:12], fileMetaData.id)

	if _, err := v.f.Write(buf); err != nil {
		panic(err)
	}
}

func (v *Version) deleteFile(level uint16, meta *FileMetaData) {
	numFiles := len(v.files[level])
	for i := 0; i < numFiles; i++ {
		if v.files[level][i].id == meta.id {
			v.files[level] = append(v.files[level][:i], v.files[level][i+1:]...)
			break
		}
	}
}

// pickCompactionLevel method    pick level that has the highest score
// if best score < 1 , if score > 0.6, try to compact highest level, if score
// for L0 score = len(files) / L0_CompactionTrigger
// for Li score = totalFileSize / maxBytesForLevel
func (v *Version) pickCompactionLevel() int {
	baseLevel := 0
	var score float64
	var bestScore float64
	var maxLevelScore float64
	for i := 0; i < v.opt.MaxLevelNum; i++ {
		if i == 0 {
			score = float64(len(v.files[0])) / float64(L0_CompactionTrigger)
		} else {
			score = float64(totalFileSize(v.files[i])) / maxBytesForLevel(i)
		}
		maxLevelScore = score
		if score > bestScore {
			bestScore = score
			baseLevel = i
		}
	}
	if bestScore < 0.6 {
		if maxLevelScore > 0.5 {
			return v.opt.MaxLevelNum - 1
		} else if len(v.files[0]) > L0_CompactionTrigger/2 {
			return 0
		}
	}
	return baseLevel
}

func maxBytesForLevel(level int) float64 {
	// result := 10. * 1048576.0 // 10M for level 1
	result := 1. * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

func totalFileSize(files []*FileMetaData) uint64 {
	var size uint64
	for _, file := range files {
		size += file.fileSize
	}
	return size
}
