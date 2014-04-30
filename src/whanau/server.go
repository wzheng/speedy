// Whanau server serves keys when it can.

package whanau

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

//import "builtin"

//import "encoding/gob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type WhanauServer struct {
	mu     sync.Mutex
	l      net.Listener
	me     int
	myaddr string
	dead   bool // for testing

	neighbors []string                  // list of servers this server can talk to
	pkvstore  map[KeyType]TrueValueType // local k/v table, used for Paxos
	kvstore   map[KeyType]ValueType     // k/v table used for routing
	ids       []KeyType                 // contains id of each layer
	fingers   [][]Finger                // (id, server name) pairs
	succ      [][]Record                // contains successor records for each layer
	db        []Record                  // sample of records used for constructing struct, according to the paper, the union of all dbs in all nodes cover all the keys =)
	pending   map[KeyType]TrueValueType // this is a list of pending writes
	view      int                       // the current view
}

func IsInList(val string, array []string) bool {
	for _, v := range array {
		if v == val {
			return true
		}
	}

	return false
}

func (ws *WhanauServer) PaxosGetRPC(args *PaxosGetArgs, reply *PaxosGetReply) error {
	// starts a new paxos log entry
	reply.Value = ""
	return nil
}

func (ws *WhanauServer) PaxosGet(key KeyType, servers ValueType) TrueValueType {
	// TODO: make a call to a random server in the group
	randIndex := rand.Intn(len(servers.Servers))
	server := servers.Servers[randIndex]
	var retries []string

	args := &PaxosGetArgs{}
	var reply PaxosGetReply

	args.Key = key

	for {
		ok := call(server, "WhanauServer.PaxosGet", args, &reply)
		if ok {
			return reply.Value
		} else {
			// try another server
			retries = append(retries, server)
			randIndex = rand.Intn(len(servers.Servers))
			server = servers.Servers[randIndex]
		}
	}

	return ""
}

// WHANAU LOOKUP HELPER METHODS

// Returns randomly chosen finger and randomly chosen layer as part of lookup
func (ws *WhanauServer) ChooseFinger(x0 KeyType, key KeyType, nlayers int) (Finger, int) {
	DPrintf("In chooseFinger, x0: %s, key: %s", x0, key)
	// find all fingers from all layers such that the key falls between x0 and the finger id
	candidateFingers := make([][]Finger, 0)
	// maps index to nonempty layer number
	layerMap := make([]int, 0)
	counter := 0
	for i := 0; i < nlayers; i++ {
		DPrintf("ws.fingers[%d]: %s", i, ws.fingers[i])
		for j := 0; j < len(ws.fingers[i]); j++ {

			// compare x0 <= id <= key on a circle
			id := ws.fingers[i][j].Id
			if x0 <= key {
				if x0 <= id && id <= key {
					if len(candidateFingers) <= counter {
						// only create non empty candidate fingers
						newLayer := make([]Finger, 0)
						candidateFingers = append(candidateFingers, newLayer)
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
					}
				}
			} else {
				// case where x0 > key, compare !(key < x < x0) --> x > x0 or x < key
				if id > x0 || id < key {
					if len(candidateFingers) < counter {
						// only create non empty candidate fingers
						newLayer := make([]Finger, 0)
						candidateFingers = append(candidateFingers, newLayer)
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
					}
				}
			}
		}
	}

	DPrintf("len(candidateFingers): %d, len(layerMap): %d", len(candidateFingers), len(layerMap))
	// pick random layer out of nonempty candidate fingers
	if len(candidateFingers) > 0 {
		randIndex := rand.Intn(len(candidateFingers))
		finger := candidateFingers[randIndex][rand.Intn(len(candidateFingers[randIndex]))]
		return finger, layerMap[randIndex]
	}

	// if can't find any, randomly choose layer and randomly return finger
	// probably shouldn't get here?
	DPrintf("cant' find finger with suitable key, pick random one instead")
	randLayer := rand.Intn(len(ws.fingers))
	randfinger := ws.fingers[randLayer][rand.Intn(len(ws.fingers[randLayer]))]
	return randfinger, randLayer
}

// Query for a key in the successor table
func (ws *WhanauServer) Query(args *QueryArgs, reply *QueryReply) error {
	layer := args.Layer
	key := args.Key
	valueIndex := sort.Search(len(ws.succ[layer]), func(i int) bool { return ws.succ[layer][i].Key == key })
	if valueIndex < len(ws.succ[layer]) {
		reply.Value = ws.succ[layer][valueIndex].Value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

// Try finds the value associated with the key
func (ws *WhanauServer) Try(args *TryArgs, reply *TryReply) error {
	key := args.Key
	DPrintf("In Try RPC, trying key: %s", key)
	fingerLength := len(ws.fingers[0])
	j := sort.Search(fingerLength, func(i int) bool {
		return ws.fingers[0][i].Id >= key
	})
	j = j % fingerLength
	if j < 0 {
		j = j + fingerLength
	}
	j = (j + fingerLength - 1) % fingerLength
	value := new(ValueType)
	for value == nil {
		f, i := ws.ChooseFinger(ws.fingers[0][j].Id, key, L)
		queryArgs := &QueryArgs{key, i}
		var queryReply QueryReply
		call(f.Address, "WhanauServer.Query", queryArgs, &queryReply)
		if queryReply.Err == OK {
			value := queryReply.Value
			reply.Value = value
			reply.Err = OK
		}
	}
	reply.Err = ErrNoKey
	return nil
}

// TODO this eventually needs to become a real lookup
func (ws *WhanauServer) Lookup(args *LookupArgs, reply *LookupReply) error {
    key := args.Key
    value := new(ValueType)
    addr := ws.myaddr
    count := 0
    for value == nil && count < 50{
        tryArgs := &TryArgs{key}
        var tryReply TryReply
        call(addr, "WhanauServer.Try", tryArgs, tryReply)
        if tryReply.Err == OK {
            value := tryReply.Value
            reply.Val = value
        } else {
            randomWalkArgs := &RandomWalkArgs{STEPS}
            var randomWalkReply RandomWalkReply
            call(ws.myaddr, "WhanauServer.RandomWalk", randomWalkArgs, randomWalkReply)
            if randomWalkReply.Err == OK {
                addr = randomWalkReply.Server
            }
        }
        count++
    }
    if value != nil {
        reply.Err = OK
    } else {
        reply.Err = ErrNoKey
    }
    return nil
}

// Client-style lookup on neighboring servers.
// routedFrom is supposed to prevent infinite lookup loops.
func (ws *WhanauServer) NeighborLookup(key KeyType, routedFrom []string) TrueValueType {
	args := &LookupArgs{}
	args.Key = key
	args.RoutedFrom = routedFrom
	var reply LookupReply
	for _, srv := range ws.neighbors {
		if IsInList(srv, routedFrom) {
			continue
		}

		ok := call(srv, "WhanauServer.Lookup", args, &reply)
		if ok && (reply.Err == OK) {
			return reply.Value
		}
	}

	return ErrNoKey
}

func (ws *WhanauServer) PaxosPutRPC(args *PaxosPutArgs, reply *PaxosPutReply) error {
	// this will initiate a new paxos call its paxos cluster
	return nil
}

func (ws *WhanauServer) PaxosPut(key KeyType, value TrueValueType) error {
	// TODO: needs to do a real paxos put
	return nil
}

// TODO this eventually needs to become a real put
func (ws *WhanauServer) Put(args *PutArgs, reply *PutReply) error {
	// TODO: needs to 1. find the paxos cluster 2. do a paxos cluster put
	// makes an RPC call to itself, this is kind of weird...

	rpc_args := &LookupArgs{}
	var rpc_reply LookupReply

	ok := call(ws.myaddr, "WhanauServer.Lookup", rpc_args, rpc_reply)

	if ok {
		if rpc_reply.Err == ErrNoKey {
			// TODO: adds the key to its local pending put list

		} else {
			// TODO: make a paxos request directly to one of the servers

		}
	}

	reply.Err = OK
	return nil
}

// Random walk
func (ws *WhanauServer) RandomWalk(args *RandomWalkArgs, reply *RandomWalkReply) error {
	steps := args.Steps
	// pick a random neighbor
	randIndex := rand.Intn(len(ws.neighbors))
	neighbor := ws.neighbors[randIndex]
	if steps == 1 {
		reply.Server = neighbor
		reply.Err = OK
	} else {
		args := &RandomWalkArgs{}
		args.Steps = steps - 1
		var rpc_reply RandomWalkReply
		ok := call(neighbor, "WhanauServer.RandomWalk", args, &rpc_reply)
		if ok && (rpc_reply.Err == OK) {
			reply.Server = rpc_reply.Server
			reply.Err = OK
		}
	}

	return nil
}

// Gets the ID from node's local id table
func (ws *WhanauServer) GetId(args *GetIdArgs, reply *GetIdReply) error {
	layer := args.Layer
	//DPrintf("In getid, len(ws.ids): %d layer: %d", len(ws.ids), layer)
	// gets the id associated with a layer
	if 0 <= layer && layer < len(ws.ids) {
		id := ws.ids[layer]
		reply.Key = id
		reply.Err = OK
	}
	return nil
}

// Whanau Routing Protocal methods

// TODO
// Populates routing table
func (ws *WhanauServer) Setup(nlayers int, rf int) {
	DPrintf("In Setup of server %s", ws.myaddr)

	// fill up db by randomly sampling records from random walks
	// "The db table has the good property that each honest node’s stored records are frequently represented in other honest nodes’db tables"
	ws.db = ws.SampleRecords(RD)

	// reset ids, fingers, succ
	ws.ids = make([]KeyType, 0)
	ws.fingers = make([][]Finger, 0)
	ws.succ = make([][]Record, 0)
	for i := 0; i < nlayers; i++ {
		// populate tables in layers
		ws.ids = append(ws.ids, ws.ChooseID(i))
		DPrintf("Finished ChooseID of server %s, layer %d", ws.myaddr, i)
		curFingerTable := ws.ConstructFingers(i, rf)
		ByFinger(FingerId).Sort(curFingerTable)
		ws.fingers = append(ws.fingers, curFingerTable)

		DPrintf("Finished ConstructFingers of server %s, layer %d", ws.myaddr, i)
		curSuccessorTable := ws.Successors(i)
		By(RecordKey).Sort(curSuccessorTable)
		ws.succ = append(ws.succ, curSuccessorTable)

		DPrintf("Finished SuccessorTable of server %s, layer %d", ws.myaddr, i)
	}
}

// return random Key/value record from local storage
func (ws *WhanauServer) SampleRecord() Record {
	randIndex := rand.Intn(len(ws.kvstore))
	keys := make([]KeyType, 0)
	for k, _ := range ws.kvstore {
		keys = append(keys, k)
	}
	key := keys[randIndex]
	value := ws.kvstore[key]
	record := Record{key, value}

	return record
}

// Returns a list of records sampled randomly from local kv store
// Note: we agreed that duplicates are fine
func (ws *WhanauServer) SampleRecords(rd int) []Record {

	records := make([]Record, 0)
	for i := 0; i < rd; i++ {
		records = append(records, ws.SampleRecord())
	}
	return records
}

// Constructs Finger table for a specified layer
func (ws *WhanauServer) ConstructFingers(layer int, rf int) []Finger {

	DPrintf("In ConstructFingers of %s, layer %d", ws.myaddr, layer)
	fingers := make([]Finger, 0)
	for i := 0; i < rf; i++ {
		steps := W // TODO: set to global W parameter
		args := &RandomWalkArgs{steps}
		reply := &RandomWalkReply{}

		// Keep trying until succeed or timeout
		// TODO add timeout later
		for reply.Err != OK {
			//DPrintf("random walk")
			ws.RandomWalk(args, reply)
		}
		server := reply.Server

		//DPrintf("randserver: %s", server)
		// get id of server using rpc call to that server
		getIdArg := &GetIdArgs{layer}
		getIdReply := &GetIdReply{}
		ok := false

		// block until succeeds
		// TODO add timeout later
		for !ok || (getIdReply.Err != OK) {
			DPrintf("rpc to getid of %s from ConstructFingers %s layer %d", server, ws.myaddr, layer)
			ok = call(server, "WhanauServer.GetId", getIdArg, getIdReply)
		}

		finger := Finger{getIdReply.Key, server}
		fingers = append(fingers, finger)
	}

	return fingers
}

// Choose id for specified layer
func (ws *WhanauServer) ChooseID(layer int) KeyType {

	DPrintf("In ChooseID of %s, layer %d", ws.myaddr, layer)
	if layer == 0 {
		// choose randomly from db
		randIndex := rand.Intn(len(ws.db))
		record := ws.db[randIndex]
		DPrintf("record.Key", record.Key)
		return record.Key

	} else {
		// choose finger randomly from layer - 1, use id of that finger
		randFinger := ws.fingers[layer-1][rand.Intn(len(ws.fingers[layer-1]))]
		return randFinger.Id
	}
}

// Defines ordering of Record args
type By func(r1, r2 *Record) bool
type ByFinger func(f1, f2 *Finger) bool

// Sort uses By to sort the Record slice
func (by By) Sort(records []Record) {
	rs := &recordSorter{
		records: records,
		by:      by,
	}
	sort.Sort(rs)
}

// sort uses By to sort fingers by id
func (by ByFinger) Sort(fingers []Finger) {
	fs := &fingerSorter{
		fingers: fingers,
		by:      by,
	}
	sort.Sort(fs)
}

// recordSorter joins a By function and a slice of Records to be sorted.
type recordSorter struct {
	records []Record
	by      func(r1, r2 *Record) bool // Closure used in the Less method.
}

// fingerSorter joins a By function and a slice of Fingers to be sorted.
type fingerSorter struct {
	fingers []Finger
	by      func(f1, f2 *Finger) bool // Closure used in the Less method
}

// Len is part of sort.Interface.
func (s *recordSorter) Len() int {
	return len(s.records)
}

func (s *fingerSorter) Len() int {
	return len(s.fingers)
}

// Swap is part of sort.Interface.
func (s *recordSorter) Swap(i, j int) {
	s.records[i], s.records[j] = s.records[j], s.records[i]
}

func (s *fingerSorter) Swap(i, j int) {
	s.fingers[i], s.fingers[j] = s.fingers[j], s.fingers[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *recordSorter) Less(i, j int) bool {
	return s.by(&s.records[i], &s.records[j])
}

func (s *fingerSorter) Less(i, j int) bool {
	return s.by(&s.fingers[i], &s.fingers[j])
}

var RecordKey = func(r1, r2 *Record) bool {
	return r1.Key < r2.Key
}

var FingerId = func(f1, f2 *Finger) bool {
	return f1.Id < f2.Id
}

// Gets successors that are nearest each key
func (ws *WhanauServer) SampleSuccessors(args *SampleSuccessorsArgs, reply *SampleSuccessorsReply) error {
	By(RecordKey).Sort(ws.db)

	key := args.Key
	t := args.T
	var records []Record
	curCount := 0
	curRecord := 0
	if t <= len(ws.db) {
		for curCount < t {
			if ws.db[curRecord].Key >= key {
				records = append(records, ws.db[curRecord])
				curCount++
			}
			curRecord++
			if curRecord == len(ws.db) {
				curRecord = 0
				key = ws.db[curRecord].Key
			}
		}
		reply.Successors = records
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (ws *WhanauServer) Successors(layer int) []Record {
	DPrintf("In Sucessors of %s, layer %d", ws.myaddr, layer)
	var successors []Record
	for i := 0; i < RS; i++ {
		args := &RandomWalkArgs{}
		args.Steps = STEPS
		reply := &RandomWalkReply{}
		ws.RandomWalk(args, reply)

		if reply.Err == OK {
			vj := reply.Server
			getIdArgs := &GetIdArgs{layer}
			getIdReply := &GetIdReply{}
			DPrintf("Calling getid layer: %d in Successors of %s", layer, ws.myaddr)
			ws.GetId(getIdArgs, getIdReply)

			sampleSuccessorsArgs := &SampleSuccessorsArgs{getIdReply.Key, NUM_SUCCESSORS}
			sampleSuccessorsReply := &SampleSuccessorsReply{}
			for sampleSuccessorsReply.Err != OK {
				call(vj, "WhanauServer.SampleSuccessors", sampleSuccessorsArgs, sampleSuccessorsReply)
			}
			successors = append(successors, sampleSuccessorsReply.Successors...)
		}
	}
	return successors
}

// tell the server to shut itself down.
func (ws *WhanauServer) kill() {
	ws.dead = true
	ws.l.Close()
	//	ws.px.Kill()
}

// TODO servers is for a paxos cluster
func StartServer(servers []string, me int, myaddr string, neighbors []string) *WhanauServer {
	ws := new(WhanauServer)
	ws.me = me
	ws.myaddr = myaddr
	ws.neighbors = neighbors

	ws.kvstore = make(map[KeyType]ValueType)
	ws.pkvstore = make(map[KeyType]TrueValueType)

	ws.view = 0

	rpcs := rpc.NewServer()
	rpcs.Register(ws)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ws.l = l

	go func() {
		for ws.dead == false {
			conn, err := ws.l.Accept()
			// removed unreliable code for now
			if err == nil && ws.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}

			if err != nil && ws.dead == false {
				fmt.Printf("ShardWS(%v) accept: %v\n", me, err.Error())
				ws.kill()
			}
		}
	}()

	// removed tick() loop for now

	return ws
}

// Methods used only for testing

// This method is only used for putting ids into the table for testing purposes
func (ws *WhanauServer) PutId(args *PutIdArgs, reply *PutIdReply) error {
	//ws.ids[args.Layer] = args.Key
	ws.ids = append(ws.ids, args.Key)
	reply.Err = OK
	return nil
}
