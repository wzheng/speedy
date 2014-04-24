// Whanau server serves keys when it can.

package whanau

import "net"
import "net/rpc"
import "log"
import "sync"
import "os"
import "fmt"
import "math/rand"
import "time"

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

	neighbors []string          // list of servers this server can talk to
	kvstore   map[string]string // local k/v table
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
func (ws *WhanauServer) RandomWalk(args *RandomWalkArgs, reply *RandomWalkReply) string {
  steps := args.Steps
  fmt.Println("In RandomWalk")
  // pick a random neighbor
  rand.Seed(time.Now().Unix())
  randIndex := rand.Intn(len(ws.neighbors))
  neighbor := ws.neighbors[randIndex]
  fmt.Println("len neighbors: " , len(ws.neighbors))
  fmt.Println("randIndex: ", randIndex)
  fmt.Println("neighbor: " , neighbor)
  if steps == 1 {
    return neighbor
  } else {
    args := &RandomWalkArgs{}
    args.Steps = steps - 1
    var reply RandomWalkReply
    ok := call(neighbor, "WhanauServer.RandomWalk", args, &reply)
    if ok && (reply.Err == OK) {
      return reply.Server
    }
  }

  return ErrRandWalk
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
