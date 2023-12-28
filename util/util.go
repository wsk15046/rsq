package util

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"math/rand"
	"time"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const lettern = 52

func init() {
	rand.Seed(time.Now().UnixNano())
}

func GetCurTs() uint64 {
	return uint64(time.Now().UnixNano() / 1e6)
}

func GetCurSec() uint32 {
	return uint32(GetCurTs() / 1000)
}

func Base64Encode(input string) string {
	return base64.StdEncoding.EncodeToString([]byte(input))
}

func Base64Decode(input string) (string, error) {
	decodeString, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return "", err
	}
	return string(decodeString), err
}
func RandString(length int) (str string) {
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(lettern)]
	}
	return string(b)
}

func MD5(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}
