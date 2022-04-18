package version

import (
	"SimpleKV/utils"
	"encoding/binary"
	"os"
)

type Version struct {
	opt    *utils.Options
	f      *os.File
	levels [][]*FileMetaData   // level -> table
	tables map[uint64]struct{} // fid -> table

	//refs int
	vset *VersionSet
	next *Version
	prev *Version
	//files []map[uint64]*FileMetaData
	files [][]*FileMetaData
}

func NewVersion(opt *utils.Options) *Version {
	//files := make([]map[uint64]*FileMetaData, opt.MaxLevelNum)
	files := make([][]*FileMetaData, opt.MaxLevelNum)
	for i := 0; i < opt.MaxLevelNum; i++ {
		//files[i] = map[uint64]*FileMetaData{}
		files[i] = make([]*FileMetaData, 0)
	}
	return &Version{
		opt:    opt,
		levels: make([][]*FileMetaData, opt.MaxLevelNum),
		files:  files,
	}
}

func (v *Version) log(level int, fileMetaData *FileMetaData, op uint16) {
	//switch op {
	//case VersionEdit_DELETE:
	//case VersionEdit_CREATE:
	//
	//}

	buf := make([]byte, 12)
	binary.BigEndian.PutUint16(buf[0:2], op)
	binary.BigEndian.PutUint16(buf[2:4], uint16(level))
	binary.BigEndian.PutUint64(buf[4:12], fileMetaData.id)
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

	//smallest := codec.EncodeKey(fileMetaData.smallest)
	//largest := codec.EncodeKey(fileMetaData.largest)
	//if _, err := v.f.Write(smallest); err != nil {
	//	panic(err)
	//}
	//if _, err := v.f.Write(largest); err != nil {
	//	panic(err)
	//}
	//log.Printf("write sst %d to level %d. smallest: %s, largest: %s\n", fileMetaData.id, level,
	//	string(fileMetaData.smallest), string(fileMetaData.largest))

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
