// Whanau server serves keys when it can.

package whanau

import "net"
import "net/rpc"
import "log"
import "sync"
import "os"
import "fmt"
import "math/rand"

//import "builtin"

//import "encoding/gob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// tuple for (id, address) pairs used in finger table
type Pair struct {
	Id      string
	Address string
}

// Key value pair
// TODO: change value later to a paxos cluster
type Record struct {
	Key   string
	Value string
}

type WhanauServer struct {
	mu     sync.Mutex
	l      net.Listener
	me     int
	myaddr string
	dead   bool // for testing

	neighbors []string          // list of servers this server can talk to
	kvstore   map[string]string // local k/v table
	ids       [L]string          // contains id of each layer
	fingers   []Pair            // (id, server name) pairs
	succ      [][]Record        // contains successor records for each layer
	db        []Record          // sample of records used for constructing struct, according to the paper, the union of all dbs in all nodes cover all the keys =)
}

func IsInList(val string, array []string) bool {
	for _, v := range array {
		if v == val {
			return true
		}
	}

	return false
}

// TODO this eventually needs to become a real lookup
func (ws *WhanauServer) Lookup(args *LookupArgs, reply *LookupReply) error {
	if val, ok := ws.kvstore[args.Key]; ok {
		reply.Value = val
		reply.Err = OK
		return nil
	}

	// probe neighbors
	// TODO eventually needs to look up based on successor table
	routedFrom := args.RoutedFrom
	routedFrom = append(routedFrom, ws.myaddr)
	neighborVal := ws.NeighborLookup(args.Key, routedFrom)

	// TODO this is a hack. NeighborLookup should be changed
	// to actually return an error.
	if neighborVal != ErrNoKey {
		reply.Value = neighborVal
		reply.Err = OK
		return nil
	}

	reply.Err = ErrNoKey
	return nil
}

// Client-style lookup on neighboring servers.
// routedFrom is supposed to prevent infinite lookup loops.
func (ws *WhanauServer) NeighborLookup(key string, routedFrom []string) string {
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

// TODO this eventually needs to become a real put
func (ws *WhanauServer) Put(args *PutArgs, reply *PutReply) error {
	ws.kvstore[args.Key] = args.Value
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
	// gets the id associated with a layer
	if 0 <= layer && layer <= len(ws.ids) {
		id := ws.ids[layer]
		reply.Key = id
		reply.Err = OK
	}
	return nil
}

// Whanau Routing Protocal methods

// TODO
// Populates routing table
func (ws *WhanauServer) Setup() {
	// fill up db

	// populate id, fingers, succ
}

// return random Key/value record from local storage
func (ws *WhanauServer) SampleRecord() Record {
	randIndex := rand.Intn(len(ws.kvstore))
	keys := make([]string, 0)
	for k, _ := range ws.kvstore {
		keys = append(keys, k)
	}
	key := keys[randIndex]
	value := ws.kvstore[key]
	record := Record{key, value}

	// TODO remove later
	fmt.Println("record: ", record)
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

func (ws *WhanauServer) ConstructFingers(layer int, rf int) []Pair {
	for i := 0; i < rf; i++ {
		steps := 2 // TODO: set to global W parameter
		args := &RandomWalkArgs{steps}
		reply := &RandomWalkReply{}
		ws.RandomWalk(args, reply)
		// TODO: need to finish after the getids rpc is made
	}
	return nil
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

	ws.kvstore = make(map[string]string)

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
	ws.ids[args.Key] = args.Value
	reply.Err = OK
	return nil
}

