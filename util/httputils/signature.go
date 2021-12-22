package httputils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

func Sign(timestamp int64, secretKey string) string {
	strToHash := fmt.Sprintf("%d\n%s", timestamp, secretKey)
	hmac256 := hmac.New(sha256.New, []byte(secretKey))
	hmac256.Write([]byte(strToHash))
	data := hmac256.Sum(nil)
	return base64.StdEncoding.EncodeToString(data)
}
