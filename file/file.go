package file

import (
	"ckv/utils/errs"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// Options
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

// FID get fid from file name
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		errs.Err(err)
		return 0
	}
	return uint64(id)
}

// FileNameSSTable  join the name of sst
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

func FileNameVLog(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.vlog", id))
}
