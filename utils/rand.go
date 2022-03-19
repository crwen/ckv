package utils

import (
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func RandN(n int) int {
	mu.Lock()
	res := r.Intn(n)
	mu.Unlock()
	return res
}
