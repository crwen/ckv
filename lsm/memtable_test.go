package lsm

import (
	"fmt"
	"testing"
)

func TestMemTableCreate(t *testing.T) {
	//MemTable{
	//
	//}
	slice := make([]byte, 1<<12)
	for i := 0; i < 2000; i++ {
		//slice = append(slice, make([]byte, 1<<12)...)
		newBuf := make([]byte, len(slice)+int(1<<12))

		slice = newBuf
		fmt.Println(len(slice) >> 12)
	}

}
