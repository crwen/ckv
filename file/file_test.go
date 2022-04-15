package file

import (
	"os"
)

var (
	opt = &Options{
		FID:      1,
		FileName: "../work_test/0001.sst",
		Dir:      "../work_test/",
		Path:     "",
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    100,
	}
)

//func TestMmap(t *testing.T) {
//	clearDir()
//
//	sst := sstable.OpenSStable(opt)
//
//	buf := make([]byte, 100)
//
//	// copy data that needed
//	copy(buf, []byte("hello world"))
//
//	// write to file
//	dst, err := sst.Bytes(0, 100)
//	if err != nil {
//		panic(err)
//	}
//	copy(dst, buf)
//
//}

func clearDir() {
	_, err := os.Stat(opt.Dir)
	if err == nil {
		os.RemoveAll(opt.Dir)
	}
	os.Mkdir(opt.Dir, os.ModePerm)
}
