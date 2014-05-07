// WhanauPaxos coordinates Puts and Gets for a particular key in a
// WhanauServer.

package whanau

import "paxos"
import "time"
import "sync"
import "math"
import "net/rpc"
import "encoding/gob"
import "fmt"

type WhanauPaxos struct {
	mu     sync.Mutex
	me     int
	dead   bool // for testing
	myaddr string

	px              *paxos.Paxos
	handledRequests map[int64]interface{}

	currSeq  int // how far in the log are we?
	logLock  sync.Mutex
	dbLock   sync.Mutex
	currView int

	db map[KeyType]TrueValueType

	// only applicable if this server is a master
	pwLock         sync.Mutex
	pending_writes map[PendingInsertsKey]string // this is a mapping from a pending write keys to servers
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

func (wp *WhanauPaxos) LogPut(args *PaxosPutArgs, reply *PaxosPutReply) {
	wp.dbLock.Lock()
	defer wp.dbLock.Unlock()

	wp.db[args.Key] = args.Value
}

// This is a Get directly from the Paxos k/v store (as opposed to a
// Lookup, that is routed along the Whanau layers).
func (wp *WhanauPaxos) LogGet(args *PaxosGetArgs, reply *PaxosGetReply) {
	wp.dbLock.Lock()
	defer wp.dbLock.Unlock()

	if getValue, ok := wp.db[args.Key]; ok {
		reply.Err = OK
		reply.Value = getValue
	} else {
		reply.Err = ErrNoKey
		reply.Value = TrueValueType{"", "", nil, nil}
	}
}

func (wp *WhanauPaxos) LogPending(args *PaxosPendingInsertsArgs, reply *PaxosPendingInsertsReply) {
	wp.pwLock.Lock()
	defer wp.pwLock.Unlock()

	v, ok := wp.pending_writes[PendingInsertsKey{args.Key, args.View}]
	if ok {
		reply.Server = v
		reply.Err = OK
	} else {
		wp.pending_writes[PendingInsertsKey{args.Key, args.View}] = args.Server
		reply.Err = OK
	}
}

// Fast forward the log from fromSeq up to toSeq, applying all the  updates.
func (wp *WhanauPaxos) LogUpdates(fromSeq int, toSeq int) {
	for i := fromSeq; i <= toSeq; i++ {
		decided, value := wp.px.Status(i)
		for !decided {
			// wait for instance to reach agreement
			// TODO should time out after a while in case too many
			// nodes have failed
			time.Sleep(time.Millisecond * 50)
			decided, value = wp.px.Status(i)
		}

		op := value.(Op)

		_, handled := wp.handledRequests[op.RequestID]
		if handled {
			// don't re-serve the request, though this shouldn't
			// be an issue
			continue
		}

		if op.Type == PUT {
			args := op.OpArgs.(PaxosPutArgs)
			var reply PaxosPutReply
			reply.Err = OK
			wp.LogPut(&args, &reply)
			wp.handledRequests[args.RequestID] = reply
		} else if op.Type == GET {
			args := op.OpArgs.(PaxosGetArgs)
			var reply PaxosGetReply
			reply.Err = OK
			wp.LogGet(&args, &reply)
			wp.handledRequests[args.RequestID] = reply
		} else if op.Type == PENDING {
			args := op.OpArgs.(PaxosPendingInsertsArgs)
			reply := PaxosPendingInsertsReply{}
			reply.Err = OK
			wp.LogPending(&args, &reply)
			wp.handledRequests[args.RequestID] = reply
		}

	}
}

func (wp *WhanauPaxos) AgreeAndLogRequests(op Op) error {
	agreedSeq := wp.RunPaxos(op)
	wp.LogUpdates(wp.currSeq, agreedSeq)
	// discard old instances
	wp.px.Done(agreedSeq)
	wp.currSeq = agreedSeq + 1

	return nil
}

func (wp *WhanauPaxos) PaxosGet(args *PaxosGetArgs,
	reply *PaxosGetReply) error {
	wp.logLock.Lock()
	defer wp.logLock.Unlock()

	// TODO check if this paxos group is responsible for this key

	// Have we handled this request already?
	if r, ok := wp.handledRequests[args.RequestID]; ok {
		getreply := r.(PaxosGetReply)

		if getreply.Err != ErrWrongGroup {
			reply.Err = getreply.Err
			reply.Value = getreply.Value
			return nil
		}
	}


	// Okay, try handling the request.
	getop := Op{GET, *args, NRand(), args.RequestID}
	wp.AgreeAndLogRequests(getop)

	getreply := wp.handledRequests[args.RequestID].(PaxosGetReply)
	reply.Err = getreply.Err
	reply.Value = getreply.Value

	fmt.Printf("get got value %v\n", getreply.Value)
	return nil
}

func (wp *WhanauPaxos) PaxosPut(args *PaxosPutArgs,
	reply *PaxosPutReply) error {
	wp.logLock.Lock()
	defer wp.logLock.Unlock()

	// TODO check if this paxos group is responsible for this key

	// Have we handled this request already?
	if r, ok := wp.handledRequests[args.RequestID]; ok {
		putreply := r.(PaxosPutReply)

		if putreply.Err != ErrWrongGroup {
			reply.Err = putreply.Err
			return nil
		}
	}

	// Okay, try handling the request.
	putop := Op{PUT, *args, NRand(), args.RequestID}
	wp.AgreeAndLogRequests(putop)

	putreply := wp.handledRequests[args.RequestID].(PaxosPutReply)
	reply.Err = putreply.Err

	return nil
}

func (wp *WhanauPaxos) PaxosPendingInsert(args *PaxosPendingInsertsArgs, reply *PaxosPendingInsertsReply) error {

	wp.logLock.Lock()
	defer wp.logLock.Unlock()

	// Have we handled this request already?
	if r, ok := wp.handledRequests[args.RequestID]; ok {
		pending_reply := r.(PaxosPendingInsertsReply)

		if pending_reply.Err != ErrWrongGroup {
			reply.Server = pending_reply.Server
			reply.Err = pending_reply.Err
			return nil
		}
	}

	// Okay, try handling the request.
	op := Op{PENDING, *args, NRand(), args.RequestID}
	wp.AgreeAndLogRequests(op)

	pending_reply := wp.handledRequests[args.RequestID].(PaxosPendingInsertsReply)

	reply.Server = pending_reply.Server
	reply.Err = pending_reply.Err

	return nil
}

func StartWhanauPaxos(servers []string, me int,
	rpcs *rpc.Server) *WhanauPaxos {

	wp := new(WhanauPaxos)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(wp)
	} else {
		rpcs := rpc.NewServer()
		rpcs.Register(wp)
	}

	wp.handledRequests = make(map[int64]interface{})
	wp.px = paxos.Make(servers, me, rpcs)
	wp.db = make(map[KeyType]TrueValueType)
	wp.pending_writes = make(map[PendingInsertsKey]string)
	wp.currSeq = 0

	gob.Register(Op{})
	gob.Register(PaxosGetArgs{})
	gob.Register(PaxosPutArgs{})
	gob.Register(PaxosGetReply{})
	gob.Register(PaxosPutReply{})
	gob.Register(PaxosPendingInsertsArgs{})
	gob.Register(PaxosPendingInsertsReply{})

	return wp
}
