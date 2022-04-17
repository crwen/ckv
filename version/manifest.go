package version

import (
	"SimpleKV/sstable"
	"SimpleKV/utils"
	"os"
	"path/filepath"
)

const (
	ManifestFilename        = "MANIFEST"
	ManifestRewriteFilename = "REWRITEMANIFEST"
)

type Manifest struct {
	opt    *utils.Options
	f      *os.File
	levels [][]*FileMetaData   // level -> table
	tables map[uint64]struct{} // fid -> table
}

// FileMetaData sstable info
type FileMetaData struct {
	//refs int
	allowedSeeks int // seeks allowed until compaction
	number       uint64
	id           uint64
	fileSize     uint64 // file size in bytes
	largest      []byte // largest key served by table
	smallest     []byte // smallest key served by table
}

func (fm *FileMetaData) UpdateMeta(t *sstable.Table) {
	fm.smallest = t.MinKey
	fm.largest = t.MaxKey
	fm.fileSize = 0
}

func NewManifest(opt *utils.Options) (*Manifest, error) {
	path := filepath.Join(opt.WorkDir, ManifestFilename)
	m := &Manifest{opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	m.f = f
	m.levels = make([][]*FileMetaData, opt.MaxLevelNum)
	m.tables = make(map[uint64]struct{})
	return m, err
}
