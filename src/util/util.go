package util

import "math/rand"

// Служит для генерации сообщения
func Random() string {
	b := make([]byte, 8)

	for i := range b {
		b[i] = byte((123 + rand.Intn(123)) * 4)
	}

	return string(b)
}
