package version

import (
	"os"
	"path/filepath"

	"ckv/sstable"
	"ckv/utils"
)

const (
	ManifestFilename        = "MANIFEST"
	VManifestFilename       = "VMANIFEST"
	ManifestRewriteFilename = "REWRITEMANIFEST"
)

type Manifest struct {
	opt    *utils.Options
	f      *os.File
	tables map[uint64]struct{}
	levels [][]*FileMetaData
}

// FileMetaData sstable info
type FileMetaData struct {
	largest      []byte
	smallest     []byte
	allowedSeeks int
	number       uint64
	id           uint64
	fileSize     uint64
}

type VFileMetaData struct {
	largest  []byte
	smallest []byte
	sstId    uint64
	fileSize uint64
	level    int
}

type VFileGroupMetaData struct {
	vfids    []uint64
	sstId    uint64
	fileSize uint64
}

func (meta *FileMetaData) UpdateMeta(t *sstable.Table) {
	meta.smallest = t.MinKey
	meta.largest = t.MaxKey
	meta.fileSize = t.Size()
	meta.id = t.Fid()
}

func NewManifest(opt *utils.Options) (*Manifest, error) {
	path := filepath.Join(opt.WorkDir, ManifestFilename)
	m := &Manifest{opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, err
	}
	m.f = f
	m.levels = make([][]*FileMetaData, opt.MaxLevelNum)
	m.tables = make(map[uint64]struct{})
	return m, err
}
