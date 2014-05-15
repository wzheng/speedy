package whanau

import "math/rand"
import (
	"crypto/sha1"
	"encoding/base64"
)

import "fmt"

//import "log"

func IsInList(val int, array []int) bool {
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

func (ws *WhanauServer) GetLookupServer() (string, bool) {
	ws.rw_mu.Lock()
	defer ws.rw_mu.Unlock()

	//fmt.Printf("asking ws %v: idx wants %d, len is %d\n",
	//	ws.me, ws.rw_idx, len(ws.rw_servers))

	if len(ws.rw_servers) <= ws.nreserved {
		// wrap around: we have reserved a certain number for lookups
		ws.lookup_idx = 0
	}

	// get nth from end
	retval := ws.rw_servers[len(ws.rw_servers)-ws.lookup_idx-1]
	ws.lookup_idx++
	return retval, true
}

// Handles getting another server from the precomputed cache of
// random walk results.
func (ws *WhanauServer) GetNextRWServer() (string, bool) {
	ws.rw_mu.Lock()
	defer ws.rw_mu.Unlock()

	//fmt.Printf("asking ws %v: idx wants %d, len is %d\n",
	//	ws.me, ws.rw_idx, len(ws.rw_servers))

	fmt.Printf("")
	if len(ws.rw_servers) <= ws.nreserved {
		// 		log.Fatalf("not enough servers in ws %v: idx wants %d, len is %d\n",
		// 			ws.me, ws.rw_idx, len(ws.rw_servers))
		return "", false
	}

	retval := ws.rw_servers[0]
	ws.rw_servers = ws.rw_servers[1:]
	return retval, true
}

func (ws *WhanauServer) FindWPInstanceIfCreated(uid string) (WhanauPaxos, bool) {
	var wp WhanauPaxos

	for _, wp = range ws.paxosInstances {
		if wp.uid == uid {
			return wp, true
		}
	}

	return wp, false
}

func getShaHash(str string) string {
	hasher := sha1.New()
	bv := []byte(str)
	hasher.Write(bv)
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	return sha
}
