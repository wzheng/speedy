// WhanauPaxos coordinates Puts and Gets for a particular key in a
// WhanauServer.

package whanau

import "paxos"
import "math/rand"
import "time"
import "sync"
import "math"
import "net/rpc"

type WhanauPaxos struct {
	mu   sync.Mutex
	me   int
	dead bool // for testing

	px              *paxos.Paxos
	handledRequests map[int64]interface{}

	currSeq int // how far in the log are we?
	logLock sync.Mutex
	dbLock  sync.Mutex
}

type Op struct {
	Type      string
	OpArgs    interface{}
	OpID      int64
	RequestID int64
}

func (wp *WhanauPaxos) RunPaxos(op Op) int {
	currSeq := wp.px.Max()

	for !wp.dead {
		wp.px.Start(currSeq, op)
		var decidedOp Op // Paxos might actually decide on some other operation

		// now we wait for the agreement to complete...
		timeout := 10 * time.Millisecond

		for {
			decided, value := wp.px.Status(currSeq)
			if decided {
				decidedOp = value.(Op)
				break
			}
			time.Sleep(timeout)

			if timeout < 10*time.Second {
				timeout *= 2
			}
		}

		// agreement completed! did we agree on the right operation?
		if decidedOp.OpID == op.OpID {
			break
		} else {
			// no-- some other replica got our instance number, so increment
			// the instance number and try again
			currSeq = int(math.Max(float64(wp.px.Max()), float64(currSeq+1)))
		}
	}

	// let the shardmaster know what instance number we actually decided on
	return currSeq
}

func (ws *WhanauServer) InitPaxosCluster(args *InitPaxosClusterArgs,
	reply *InitPaxosClusterReply) error {
	if args.Phase == PhaseOne {
		reply.Reply = Commit
	} else {
		if args.Action == Commit {
			for k, v := range args.KeyMap {
				ws.pkvstore[k] = v
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

func StartWhanauPaxos(servers []string, me int) *WhanauPaxos {

	wp := new(WhanauPaxos)

	rpcs := rpc.NewServer()
	rpcs.Register(wp)

	wp.handledRequests = make(map[int64]interface{})
	wp.px = paxos.Make(servers, me, rpcs)

	return wp
}
