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
	"time"
)

//import "builtin"

//import "encoding/gob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type WhanauServer struct {
	//// Server variables ////
	mu     sync.Mutex
	l      net.Listener
	me     int
	myaddr string
	dead   bool // for testing

	//// Paxos variables ////
	// map of key -> local WhanauPaxos instance handling the key
	// WhanauPaxos instance handles communication with other replicas
	paxosInstances map[KeyType]WhanauPaxos

	//// Routing variables ////
	neighbors []string              // list of servers this server can talk to
	kvstore   map[KeyType]ValueType // k/v table used for routing
	ids       []KeyType             // contains id of each layer
	fingers   [][]Finger            // (id, server name) pairs
	succ      [][]Record            // contains successor records for each layer
	db        []Record              // sample of records used for constructing struct, according to the paper, the union of all dbs in all nodes cover all the keys =)

	master    []string                  // list of servers for the master cluster; these servers are also trusted

	is_master bool                      // whether the server itself is a master server
	state     State                     // what phase the server is in
	pending   map[KeyType]TrueValueType // this is a list of pending writes
}

type WhanauSybilServer struct {
	WhanauServer
	sybilNeighbors []WhanauSybilServer
}

// for testing
func (ws *WhanauServer) AddToKvstore(key KeyType, value ValueType) {
	ws.kvstore[key] = value
}

func (ws *WhanauServer) GetDB() []Record {
	return ws.db
}

func (ws *WhanauServer) GetSucc() [][]Record {
	return ws.succ
}

// RPC to actually do a Get on the server's WhanauPaxos cluster.
// Essentially just passes the call on to the WhanauPaxos servers.
func (ws *WhanauServer) PaxosGetRPC(args *PaxosGetArgs,
	reply *PaxosGetReply) error {

	if _, ok := ws.paxosInstances[args.Key]; !ok {
		reply.Err = ErrNoKey
		return nil
	}

	instance := ws.paxosInstances[args.Key]
	instance.PaxosGet(args, reply)

	return nil
}

// RPC to actually do a Put on the server's WhanauPaxos cluster.
// Essentially just passes the call on to the WhanauPaxos servers.
func (ws *WhanauServer) PaxosPutRPC(args *PaxosPutArgs,
	reply *PaxosPutReply) error {
	// this will initiate a new paxos call its paxos cluster
	if _, ok := ws.paxosInstances[args.Key]; !ok {
		reply.Err = ErrNoKey
		return nil
	}

	instance := ws.paxosInstances[args.Key]
	instance.PaxosPut(args, reply)

	return nil
}

func (ws *WhanauServer) AddPendingRPC(args *PendingArgs,
	reply *PendingReply) error {
	ws.pending[args.Key] = args.Value
	reply.Err = ErrPending

	return nil
}

// Returns paxos cluster for given key.
// Called by servers to figure out which paxos cluster
// to get the true value from.
func (ws *WhanauServer) Lookup(args *LookupArgs, reply *LookupReply) error {
	key := args.Key
	nlayers := args.NLayers
	steps := args.Steps

	DPrintf("In Lookup key: %s server %s", key, ws.myaddr)
	addr := ws.myaddr
	count := 0
	tryArgs := &TryArgs{key, nlayers}
	tryReply := &TryReply{}

	for tryReply.Err != OK && count < TIMEOUT {
		call(addr, "WhanauServer.Try", tryArgs, tryReply)
		randomWalkArgs := &RandomWalkArgs{steps}
		randomWalkReply := &RandomWalkReply{}
		call(ws.myaddr, "WhanauServer.RandomWalk", randomWalkArgs, randomWalkReply)
		if randomWalkReply.Err == OK {
			addr = randomWalkReply.Server
		}
		count++
	}

	if tryReply.Err == OK {
		value := tryReply.Value
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	return nil
}

// Get the value for a particular key.
// Runs Lookup to find the appropriate Paxos cluster, then
// calls Get on that cluster.
// TODO needs to check if wrong view/group
// Sybil node lookup returns false value with some probability
func (ws *WhanauSybilServer) Lookup(args *LookupArgs, reply *LookupReply) error {
	key := args.Key
	steps := args.Steps

	r := rand.New(rand.NewSource(99))
	prob := r.Float32()
	if prob > 0.6 {
		reply.Err = ErrNoKey
	} else {
		valueIndex := sort.Search(len(ws.db), func(valueIndex int) bool {
			return ws.db[valueIndex].Key >= key
		})

		if valueIndex < len(ws.db) && ws.db[valueIndex].Key == key {
			reply.Value = ws.db[valueIndex].Value
			reply.Err = OK
		} else {
			if steps > 0 {
				for i := 0; i < len(ws.neighbors); i++ {
					lookupArgs := &LookupArgs{}
					lookupArgs.Key = key
					lookupArgs.Steps = steps - 1
					call(ws.sybilNeighbors[i].myaddr, "WhanauSybilServer.Lookup", lookupArgs, reply)
					if reply.Err == OK {
						break
					}
				}
			} else {
				reply.Err = ErrNoKey
			}
		}
	}
	return nil
}

func (ws *WhanauServer) InitPaxosCluster(args *InitPaxosClusterArgs,
	reply *InitPaxosClusterReply) error {
	if args.Phase == PhaseOne {
		reply.Reply = Commit
	} else {
		if args.Action == Commit {
			for k, _ := range args.KeyMap {
				var value ValueType
				value.Servers = args.Servers
				ws.kvstore[k] = value
			}
		} else {
			// do nothing?
		}
	}

	reply.Err = OK
	return nil
}

func (ws *WhanauServer) ConstructPaxosCluster() []string {

	var cluster []string

	randIndex := rand.Intn(len(ws.neighbors))
	neighbor := ws.neighbors[randIndex]

	// pick several random walk nodes to join the Paxos cluster
	for i := 0; i < PaxosSize; i++ {
		args := &RandomWalkArgs{}
		args.Steps = PaxosWalk
		var reply RandomWalkReply
		ok := call(neighbor, "WhanauServer.RandomWalk", args, &reply)
		if ok && (reply.Err == OK) {
			cluster = append(cluster, reply.Server)
		}
	}

	// initiate 2PC with all the nodes in the tentative paxos cluster
	// pass key-value information in the second phase, if it's okay to commit

	var if_commit = true

	for c := range cluster {
		args := &InitPaxosClusterArgs{}
		var reply InitPaxosClusterReply
		args.RequestServer = ws.myaddr
		args.Phase = PhaseOne
		args.Action = ""
		ok := call(cluster[c], "WhanauServer.InitPaxosCluster", args, &reply)
		if ok && (reply.Err == OK) {
			if reply.Reply == Reject {
				if_commit = false
				break
			}
		}
	}

	// Send commit message to every server, along with the key
	for c := range cluster {
		args := &InitPaxosClusterArgs{}
		var reply InitPaxosClusterReply
		args.RequestServer = ws.myaddr
		args.Phase = PhaseTwo

		if if_commit {
			args.Action = Commit
		} else {
			args.Action = Abort
		}

		ok := call(cluster[c], "WhanauServer.InitPaxosCluster", args, &reply)
		if ok && (reply.Err == OK) {
		}
	}

	return cluster
}

// Server for Sybil nodes
func (ws *WhanauSybilServer) SetupSybil(rd int, w int, neighbors []WhanauSybilServer) {
	DPrintf("In Setup of Sybil server %s", ws.myaddr)

	// Fill up table but might not use values
	ws.db = ws.SampleRecords(rd, w)

	// No need for other variables because Sybil nodes will be routing to other Sybil nodes
	ws.sybilNeighbors = neighbors
}

// return random Key/value record from local storage
func (ws *WhanauServer) SampleRecord(args *SampleRecordArgs, reply *SampleRecordReply) error {
	randIndex := rand.Intn(len(ws.kvstore))
	keys := make([]KeyType, 0)
	for k, _ := range ws.kvstore {
		keys = append(keys, k)
	}
	key := keys[randIndex]
	value := ws.kvstore[key]
	record := Record{key, value}

	reply.Record = record
	reply.Err = OK
	return nil
}

// Returns a list of records sampled randomly from local kv store
// Note: we agreed that duplicates are fine
func (ws *WhanauServer) SampleRecords(rd int, steps int) []Record {

	records := make([]Record, 0)
	for i := 0; i < rd; i++ {
		// random walk
		rwargs := &RandomWalkArgs{steps}
		rwreply := &RandomWalkReply{}
		counter := 0
		for rwreply.Err != OK && counter < TIMEOUT {
			ws.RandomWalk(rwargs, rwreply)
			counter++
		}
		server := rwreply.Server
		// Do rpc call to samplerecord
		srargs := &SampleRecordArgs{}
		srreply := &SampleRecordReply{}
		counter = 0
		for srreply.Err != OK && counter < TIMEOUT {
			call(server, "WhanauServer.SampleRecord", srargs, srreply)
		}

		if srreply.Err == OK {
			records = append(records, srreply.Record)
		}
	}
	return records
}

// Constructs Finger table for a specified layer
func (ws *WhanauServer) ConstructFingers(layer int, rf int, steps int) []Finger {

	DPrintf("In ConstructFingers of %s, layer %d", ws.myaddr, layer)
	fingers := make([]Finger, 0)
	for i := 0; i < rf; i++ {
		args := &RandomWalkArgs{steps}
		reply := &RandomWalkReply{}

		// Keep trying until succeed or timeout
		//counter := 0
		for reply.Err != OK {
			ws.RandomWalk(args, reply)
			//counter++
		}

		server := reply.Server

		// get id of server using rpc call to that server
		getIdArg := &GetIdArgs{layer}
		getIdReply := &GetIdReply{}
		ok := false

		// block until succeeds
		//counter = 0
		for !ok || (getIdReply.Err != OK) {
			DPrintf("rpc to getid of %s from ConstructFingers %s layer %d", server, ws.myaddr, layer)
			ok = call(server, "WhanauServer.GetId", getIdArg, getIdReply)
			//counter++
		}

		if getIdReply.Err == OK {
			finger := Finger{getIdReply.Key, server}
			fingers = append(fingers, finger)
		}
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

func (ws *WhanauServer) Successors(layer int, steps int, rs int, nsuccessors int) []Record {
	DPrintf("In Sucessors of %s, layer %d", ws.myaddr, layer)
	var successors []Record
	for i := 0; i < rs; i++ {
		args := &RandomWalkArgs{}
		args.Steps = steps
		reply := &RandomWalkReply{}
		ws.RandomWalk(args, reply)

		if reply.Err == OK {
			vj := reply.Server
			getIdArgs := &GetIdArgs{layer}
			getIdReply := &GetIdReply{}
			DPrintf("Calling getid layer: %d in Successors of %s", layer, ws.myaddr)
			ws.GetId(getIdArgs, getIdReply)

			sampleSuccessorsArgs := &SampleSuccessorsArgs{getIdReply.Key, nsuccessors}
			sampleSuccessorsReply := &SampleSuccessorsReply{}
			for sampleSuccessorsReply.Err != OK {
				call(vj, "WhanauServer.SampleSuccessors", sampleSuccessorsArgs, sampleSuccessorsReply)
			}
			successors = append(successors, sampleSuccessorsReply.Successors...)
		}
	}
	return successors
}

// each server changes its current state, and sends the setup messages
// to all of its neighbors
func (ws *WhanauServer) StartSetup(args *StartSetupArgs, reply *StartSetupReply) error {
	ws.mu.Lock()
	if ws.state == Normal {
		ws.state = PreSetup
	}
	ws.mu.Unlock()

	// forward this msg to all of its neighbors
	for _, srv := range ws.neighbors {
		rpc_args := &StartSetupArgs{args.MasterServer}
		rpc_reply := &StartSetupReply{}
		ok := call(srv, "WhanauServer.StartSetup", rpc_args, rpc_reply)
		if ok {

		}
	}

	if ws.state == PreSetup {
		// send its pending keys to one of the random master nodes
		for {
			randIndex := rand.Intn(len(ws.master))
			master_server := ws.master[randIndex]
			rpc_args := &ReceivePendingWritesArgs{ws.pending}
			rpc_reply := &ReceivePendingWritesReply{}
			ok := call(master_server, "WhanauServer.ReceivePendingWrites", rpc_args, rpc_reply)
			if ok {
				if rpc_reply.Err == OK {
					ws.mu.Lock()
					ws.state = Setup
					ws.mu.Unlock()
					break
				}
			}
		}
	}
	return nil
}

func (ws *WhanauServer) ReceivePendingWrites(args *ReceivePendingWritesArgs, reply *ReceivePendingWritesReply) error {
	// store the pending writes to its own pending
	ws.mu.Lock()
	for k, v := range args.PendingWrites {
		ws.pending[k] = v
	}
	ws.mu.Unlock()
	reply.Err = OK
	return nil
}

// this function shoud be run in a separate thread
// by each master server
func (ws *WhanauServer) InitiateSetup() {
	for {
		if ws.state == Normal {
			// send a start setup message to each one of its neighbors
			for _, srv := range ws.neighbors {
				args := &StartSetupArgs{ws.myaddr}
				reply := &StartSetupReply{}
				ok := call(srv, "WhanauServer.StartSetup", args, reply)
				if ok {
					//pending_writes := reply.PendingWrites
				}
			}
		}

		time.Sleep(180 * time.Second)
	}
}

// tell the server to shut itself down.
func (ws *WhanauServer) Kill() {
	ws.dead = true
	ws.l.Close()
	//	ws.px.Kill()
}

// TODO servers is for a paxos cluster
func StartServer(servers []string, me int, myaddr string,
	neighbors []string) *WhanauServer {
	ws := new(WhanauServer)
	ws.me = me
	ws.myaddr = myaddr
	ws.neighbors = neighbors

	ws.kvstore = make(map[KeyType]ValueType)
	ws.state = Normal

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
				ws.Kill()
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
