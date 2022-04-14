package file

import (
	"os"
	"testing"
)

func TestMmap(t *testing.T) {
	f, err := os.OpenFile("tmp.txt", os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		panic(err)
	}
	msg := []byte("hello world")

	buf, err := Mmap(f, true, int64(len(msg)*2))
	if err != nil {
		panic(err)
	}
	f.Write(msg)
	defer Munmap(buf)
}
