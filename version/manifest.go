package version

import (
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
	levels map[uint64]uint8    // fid -> level
	tables map[uint64]struct{} // fid -> table
}

func NewManifest(opt *utils.Options) (*Manifest, error) {
	path := filepath.Join(opt.WorkDir, ManifestFilename)
	m := &Manifest{opt: opt}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	m.f = f
	m.levels = make(map[uint64]uint8)
	m.tables = make(map[uint64]struct{})
	return m, err
}
