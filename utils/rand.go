package utils

import (
	"fmt"
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

// 生成随机字符串作为key和value
func randStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

// 构建entry对象
func BuildEntry() *Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678牛马"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}
