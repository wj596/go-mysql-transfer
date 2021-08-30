package cryptoutils

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func MD5(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}

func HmacSHA256(plaintext string, key string) string {
	hash := hmac.New(sha256.New, []byte(key)) // 创建哈希算法
	hash.Write([]byte(plaintext))             // 写入数据
	return fmt.Sprintf("%X", hash.Sum(nil))
}

func HmacMD5(plaintext string, key string) string {
	hash := hmac.New(md5.New, []byte(key)) // 创建哈希算法
	hash.Write([]byte(plaintext))          // 写入数据
	return fmt.Sprintf("%X", hash.Sum(nil))
}
