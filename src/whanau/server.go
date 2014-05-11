// Whanau server serves keys when it can.

package whanau

import (
	crand "crypto/rand"
	"crypto/rsa"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
)

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
	reqID  int64
	rpc    *rpc.Server

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

	rw_servers []string // list of random walk servers from systolic mixing
	rw_idx     int64
	rw_mu      sync.Mutex
	rec_mu     sync.Mutex
	recv_chan  chan *SystolicMixingArgs
	doneMixing bool

	masters []string // list of servers for the master cluster; these servers are also trusted

	is_master bool                      // whether the server itself is a master server
	is_sybil  bool                      // whether the server is a sybil server
	state     State                     // what phase the server is in
	pending   map[KeyType]TrueValueType // this is a list of pending writes

	// for master server only
	master_paxos_cluster WhanauPaxos                         // the paxos cluster for master servers
	all_pending_writes   map[PendingInsertsKey]TrueValueType // all of the current pending writes that it has
	key_to_server        map[PendingInsertsKey]string        // the server for a particular key
	new_paxos_clusters   [][]string                          // all of the new paxos clusters constructed in the current view

	//// DATA INTEGRITY FIELDS ////
	secretKey *rsa.PrivateKey

	//// Parameters for routing ////
	// n = number of honest nodes
	// m = O(n) number of honest edges
	// k = number of keys/node

	nlayers int // nlayers = number of layers, O(log(km))
	rf      int // rf = size of finger table, O(sqrt(km))
	w       int // w = number of steps in random walk, mixing time
	rd      int // rd = size of database, O(sqrt(km))
	rs      int // rs = number of nodes to collect samples from, O(sqrt(km))
	t       int // t = number of successors returned from sample per node, less than rs

	// Systolic mixing variables
	received_servers map[int][][]string // timestep -> neighbor name -> values
	nreserved        int                // num in pool reserved for Lookups
	lookup_idx       int
}

type WhanauSybilServer struct {
	WhanauServer
	sybilNeighbors []string // List of all other sybil servers
}

// for testing
func (ws *WhanauServer) AddToKvstore(key KeyType, value ValueType) {
	if !ws.is_sybil {
		ws.kvstore[key] = value
	}
}

func (ws *WhanauServer) GetDB() []Record {
	return ws.db
}

func (ws *WhanauServer) GetSucc() [][]Record {
	return ws.succ
}

// RPC to actually do a Get on the server's WhanauPaxos cluster.
// Essentially just passes the call on to the WhanauPaxos servers.
func (ws *WhanauServer) PaxosGetRPC(args *ClientGetArgs,
	reply *ClientGetReply) error {

	if _, ok := ws.paxosInstances[args.Key]; !ok {
		reply.Err = ErrNoKey
	}

	get_args := PaxosGetArgs{args.Key, args.RequestID}
	var get_reply PaxosGetReply

	instance := ws.paxosInstances[args.Key]
	instance.PaxosGet(&get_args, &get_reply)

	if VerifyTrueValue(get_reply.Value) {
		reply.Value = get_reply.Value.TrueValue
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrFailVerify
	}
	return nil
}

// RPC to actually do a Put on the server's WhanauPaxos cluster.
// Essentially just passes the call on to the WhanauPaxos servers.
func (ws *WhanauServer) PaxosPutRPC(args *ClientPutArgs,
	reply *ClientPutReply) error {
	// this will initiate a new paxos call its paxos cluster
	if _, ok := ws.paxosInstances[args.Key]; !ok {
		reply.Err = ErrNoKey
		return nil
	}

	put_args := PaxosPutArgs{args.Key, args.Value, args.RequestID}
	var put_reply PaxosPutReply

	instance := ws.paxosInstances[args.Key]
	instance.PaxosPut(&put_args, &put_reply)

	reply.Err = put_reply.Err

	return nil
}

func (ws *WhanauServer) AddPendingRPCMaster(args *PendingArgs, reply *PendingReply) error {

	// TODO: put the paxos call in another function, so we don't block

	var current_view int

	ws.mu.Lock()
	ws.all_pending_writes[PendingInsertsKey{args.Key, ws.master_paxos_cluster.currView}] = args.Value
	ws.mu.Unlock()

	rpc_args := &PaxosPendingInsertsArgs{args.Key, current_view, args.Server, NRand()}
	rpc_reply := &PaxosPendingInsertsReply{}
	ws.master_paxos_cluster.PaxosPendingInsert(rpc_args, rpc_reply)

	ws.mu.Lock()
	ws.key_to_server[PendingInsertsKey{args.Key, ws.master_paxos_cluster.currView}] = rpc_reply.Server
	ws.mu.Unlock()

	reply.Err = OK
	return nil
}

func (ws *WhanauServer) AddPendingRPC(args *PendingArgs,
	reply *PendingReply) error {

	// this will add a pending write to one of the master nodes

	for _, server := range ws.masters {
		rpc_reply := &PendingReply{}
		ok := call(server, "WhanauServer.AddPendingRPCMaster", args, rpc_reply)
		if ok {
			if rpc_reply.Err == OK {
				reply.Err = ErrPending
			} else {
				reply.Err = rpc_reply.Err
			}
		}
	}

	return nil
}

// tell the server to shut itself down.
func (ws *WhanauServer) Kill() {
	ws.dead = true
	ws.l.Close()
	//	ws.px.Kill()
}

// TODO servers is for a paxos cluster
func StartServer(servers []string, me int, myaddr string,
	neighbors []string, masters []string, is_master bool, is_sybil bool,
	nlayers int, rf int, w int, rd int, rs int, t int) *WhanauServer {

	ws := new(WhanauServer)
	ws.me = me
	ws.myaddr = myaddr
	ws.neighbors = neighbors

	ws.kvstore = make(map[KeyType]ValueType)
	ws.state = Normal
	ws.reqID = 0

	ws.masters = masters
	ws.is_master = is_master
	ws.is_sybil = is_sybil

	if is_master {

		var idx int

		for i, m := range masters {
			if m == ws.myaddr {
				idx = i
				break
			}
		}

		wp_m := StartWhanauPaxos(masters, idx, ws.rpc)
		ws.master_paxos_cluster = *wp_m
		ws.all_pending_writes = make(map[PendingInsertsKey]TrueValueType)
		ws.key_to_server = make(map[PendingInsertsKey]string)
		ws.new_paxos_clusters = make([][]string, 0)
	}

	// whanau routing parameters
	ws.nlayers = nlayers
	ws.rf = rf
	ws.w = w
	ws.rd = rd
	ws.rs = rs
	ws.t = t

	ws.received_servers = make(map[int][][]string, ws.w+1)
	ws.rw_servers = make([]string, 0)
	ws.rw_idx = 0
	ws.recv_chan = make(chan *SystolicMixingArgs)
	ws.doneMixing = true
	ws.nreserved = int(math.Pow(float64(ws.rd), 2))
	ws.lookup_idx = 0

	ws.paxosInstances = make(map[KeyType]WhanauPaxos)

	ws.rpc = rpc.NewServer()
	ws.rpc.Register(ws)

	gob.Register(LookupArgs{})
	gob.Register(LookupReply{})
	gob.Register(PendingArgs{})
	gob.Register(PendingReply{})
	gob.Register(PaxosGetArgs{})
	gob.Register(PaxosGetReply{})
	gob.Register(PaxosPutArgs{})
	gob.Register(PaxosPutReply{})
	gob.Register(JoinClusterArgs{})
	gob.Register(JoinClusterReply{})
	gob.Register(SystolicMixingArgs{})
	gob.Register(SystolicMixingReply{})

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ws.l = l

	// Generate secret/public key
	sk, err := rsa.GenerateKey(crand.Reader, 2014)

	if err != nil {
		log.Fatal("key generation err: ", err)
	}
	ws.secretKey = sk

	go func() {
		for ws.dead == false {
			conn, err := ws.l.Accept()
			// removed unreliable code for now
			if err == nil && ws.dead == false {
				go ws.rpc.ServeConn(conn)
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

// This method is only used for putting ids into the table
// for testing purposes
func (ws *WhanauServer) PutId(args *PutIdArgs, reply *PutIdReply) error {
	//ws.ids[args.Layer] = args.Key
	ws.ids = append(ws.ids, args.Key)
	reply.Err = OK
	return nil
}

func (ws *WhanauServer) WhanauPutRPC(args *WhanauPutRPCArgs, reply *WhanauPutRPCReply) error {

	key := args.Key
	v := args.Value

	value := TrueValueType{v, ws.myaddr, nil, &ws.secretKey.PublicKey}
	value.Sign, _ = SignTrueValue(value, ws.secretKey)

	lookup_args := &LookupArgs{}
	lookup_reply := &LookupReply{}

	lookup_args.Key = key

	var err Err
	var servers []string

	ws.Lookup(lookup_args, lookup_reply)

	if lookup_reply.Err != ErrNoKey {
		servers = lookup_reply.Value.Servers
	}
	err = lookup_reply.Err

	if err == ErrNoKey {
		// TODO: adds the key to its local pending put list
		// TODO: what happens if a client makes a call to insert
		// the same key to 2 different servers? or 2 different clients
		// making 2 different calls to the same key?
		pending_args := &PendingArgs{key, value, ws.myaddr}
		pending_reply := &PendingReply{}

		ws.AddPendingRPC(pending_args, pending_reply)

	} else {

		cpargs := &ClientPutArgs{key, value, NRand(), ws.myaddr}
		cpreply := &ClientPutReply{}

		randIdx := rand.Intn(len(servers))

		ok := call(servers[randIdx], "WhanauServer.PaxosPutRPC", cpargs, cpreply)
		if ok {
			reply.Err = cpreply.Err
		}
	}

	return nil
}
