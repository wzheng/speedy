package whanau

import "math/rand"

func IsInList(val string, array []string) bool {
	for _, v := range array {
		if v == val {
			return true
		}
	}

	return false
}

// Returns the index of the first record with key >= k.
// Circular, so if k is larger than any element, will return 0.
func PositionOf(k KeyType, array []Record) int {
	for i, v := range array {
		if v.Key >= k {
			return i
		}
	}

	return 0
}

func Shuffle(src []string) []string {
	dest := make([]string, len(src))
	perm := rand.Perm(len(src))
	for i, v := range perm {
		dest[v] = src[i]
	}

	return dest
}
