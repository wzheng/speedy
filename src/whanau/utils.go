package whanau

import "math/rand"

//import "fmt"
import "log"

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

// Handles getting another server from the precomputed cache of
// random walk results.
func (ws *WhanauServer) GetNextRWServer() (string, bool) {
	ws.rw_mu.Lock()
	defer ws.rw_mu.Unlock()

	//fmt.Printf("asking ws %v: idx wants %d, len is %d\n",
	//	ws.me, ws.rw_idx, len(ws.rw_servers))

	if len(ws.rw_servers) == 0 {
		log.Fatalf("not enough servers in ws %v: idx wants %d, len is %d\n",
			ws.me, ws.rw_idx, len(ws.rw_servers))
		return "", false
	}

	retval := ws.rw_servers[0]
	ws.rw_servers = ws.rw_servers[1:]
	return retval, true

	if ws.rw_idx >= int64(len(ws.rw_servers)) {
		log.Fatalf("not enough servers in ws %v: idx wants %d, len is %d\n",
			ws.me, ws.rw_idx, len(ws.rw_servers))
		return "", false
	}

	retval = ws.rw_servers[ws.rw_idx]
	ws.rw_idx = ws.rw_idx + 1

	return retval, true
}
