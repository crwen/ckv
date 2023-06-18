package lsm

import (
	"ckv/file"
	"ckv/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func initOpt() *file.Options {

	workDir := "../work_test"
	var fid uint64 = 1

	return &file.Options{
		Dir:      workDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    1 << 14,
		FID:      fid,
		FileName: mtFilePath(workDir, fid),
	}
}

func buildEntry() *utils.Entry {
	return utils.BuildEntry()
}

func TestWalCreate(t *testing.T) {
	clearDir()

	options := initOpt()
	wal := OpenWalFile(options)
	assert.NotNil(t, wal)

	clearDir()
}

func TestWalFileWrite(t *testing.T) {
	clearDir()

	options := initOpt()
	wal := OpenWalFile(options)
	assert.NotNil(t, wal)

	ent := buildEntry()
	err := wal.Write(ent)
	assert.Nil(t, err)

	wal.Iterate(func(e *utils.Entry) error {
		//assert.Equal(t, ent.Key, e.Key)
		//assert.Equal(t, ent.Value, e.Value)
		fmt.Println(ent.Key, ent.Value)
		fmt.Println(e.Key, e.Value)
		return nil
	})

	clearDir()
}
