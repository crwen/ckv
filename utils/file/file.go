package file

import (
	"SimpleKV/utils"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

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
		utils.Err(err)
		return 0
	}
	return uint64(id)
}

// FileNameSSTable  join the name of sst
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}
