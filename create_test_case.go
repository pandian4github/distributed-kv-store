package main

import (
	"time"
	"math/rand"
	"fmt"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main () {
	keySize := 10
	valueSize := 20
	client := 5
	for i := 0; i < 500; i++ {
		key := RandStringRunes(keySize)
		value := RandStringRunes(valueSize)
		fmt.Println("put", client, key, value)
		client++
		if client > 9 {
			client = 5
		}
	}
}