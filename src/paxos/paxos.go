package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	proposelock sync.Mutex

	instances map[int]Instance

	my_done     int // my client's Done value
	done_values map[string]int
	min_done    float64 // min Done of all peers. float64 for comparisons
	seq         int     // highest seq instance seen thus far

	ref_time time.Time // reference time for calculating n
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v for server %v\n", err1, srv)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) DoPrepareRound(seq int, value interface{},
	proposalNum int64) (ok bool, nextVal interface{}) {

	args := PrepareArgs{seq, proposalNum, px.my_done}
	var reply PrepareReply

	n_ok := 0
	var nextNum int64 = -1
	nextVal = value

	for _, peer := range px.peers {
		var all_ok bool = true

		// Send prepare(n) to all servers.
		if peer == px.peers[px.me] {
			px.Prepare(&args, &reply)
			all_ok = true
		} else {
			all_ok = call(peer, "Paxos.Prepare", &args, &reply)
		}

		if all_ok && reply.OK {
			px.handleDoneMessage(peer, reply.Done)
			n_ok += 1

			if reply.HighestProposalSeen > nextNum {
				// update the value with the highest value seen.
				nextVal = reply.HighestValueSeen
				nextNum = reply.HighestProposalSeen
			}
		}
	}

	time.Sleep(time.Millisecond * 50)

	ok = false
	if n_ok >= (len(px.peers)+1)/2. {
		ok = true
	}

	return ok, nextVal
}

func (px *Paxos) DoAcceptRound(seq int, value interface{},
	proposalNum int64) (ok bool) {
	n_accept := 0

	for _, peer := range px.peers {
		var all_ok bool = true
		a_args := AcceptArgs{seq, proposalNum, value, px.my_done}
		var a_reply AcceptReply

		if peer == px.peers[px.me] {
			px.Accept(&a_args, &a_reply)
			all_ok = true
		} else {
			all_ok = call(peer, "Paxos.Accept", &a_args, &a_reply)
		}

		if all_ok && a_reply.OK {
			px.handleDoneMessage(peer, a_reply.Done)
			n_accept += 1
		}
	}

	ok = false
	if n_accept >= (len(px.peers)+1)/2. {
		ok = true
	}

	return ok
}

func (px *Paxos) DoDecidedRound(seq int, value interface{},
	proposalNum int64) {

	d_args := DecidedArgs{seq, proposalNum, value, px.my_done}
	var d_reply DecidedReply
	for _, peer := range px.peers {
		if peer == px.peers[px.me] {
			px.Decided(&d_args, &d_reply)
		} else {
			call(peer, "Paxos.Decided", &d_args, &d_reply)
		}
	}
}

func (px *Paxos) Propose(seq int, value interface{}) {
	px.proposelock.Lock()
	defer px.proposelock.Unlock()

	for !px.dead {
		// Choose n, unique and higher than any n seen so far.
		var proposalNum int64
		t := (time.Now().Sub(px.ref_time)).Nanoseconds()
		proposalNum = t*int64(len(px.peers))*10 + int64(px.me)

		prepare_success, newValue := px.DoPrepareRound(seq,
			value, proposalNum)

		if prepare_success {
			accept_success := px.DoAcceptRound(seq, newValue, proposalNum)

			if accept_success {
				px.DoDecidedRound(seq, newValue, proposalNum)
				break
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq > px.seq {
		px.seq = seq
	}

	// Spawn a goroutine and return.
	go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	if seq > px.my_done {
		px.my_done = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.seq
}

// Update our minimum Done values.
func (px *Paxos) handleDoneMessage(peer string, seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	min_done := math.Inf(1)

	for _, peerval := range px.done_values {
		if float64(peerval) < min_done {
			min_done = float64(peerval)
		}
	}

	px.min_done = min_done

	var i float64 = -1
	for ; i < min_done; i++ {
		delete(px.instances, int(i))
	}

	px.done_values[peer] = int(math.Max(float64(px.done_values[peer]), float64(seq)))
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return int(px.min_done + 1)
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	if instance, ok := px.instances[seq]; ok && instance.decided {
		return instance.decided, instance.v_decided
	} else {
		if seq < px.my_done {
			// This is a stale sequence number. It shouldn't matter what
			// gets returned.
			return true, nil
		}
	}

	return false, nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	existingInstance, ok := px.instances[args.Seq]

	if !ok {
		reply.OK = true
		reply.HighestProposalSeen = -1

		newInstance := Instance{args.ProposalNum, -1, nil, false, nil}
		px.instances[args.Seq] = newInstance
	} else {
		if args.ProposalNum > existingInstance.h_prepare {
			newInstance := px.instances[args.Seq]
			newInstance.h_prepare = args.ProposalNum
			px.instances[args.Seq] = newInstance

			reply.HighestProposalSeen = existingInstance.h_accept
			reply.HighestValueSeen = existingInstance.h_value

			reply.OK = true
		} else {
			reply.OK = false
		}

	}

	reply.Done = px.my_done
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.OK = false
	existingInstance := px.instances[args.Seq]
	if args.ProposalNum >= existingInstance.h_prepare {
		newInstance := Instance{args.ProposalNum, args.ProposalNum,
			args.ValueToAccept, existingInstance.decided,
			existingInstance.v_decided}
		px.instances[args.Seq] = newInstance
		reply.OK = true
	}

	reply.Done = px.my_done

	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if (args.ProposalNum <= px.instances[args.Seq].h_prepare) &&
		(args.Seq >= px.my_done) {
		newInstance := px.instances[args.Seq]
		newInstance.v_decided = args.DecidedValue
		newInstance.decided = true

		px.instances[args.Seq] = newInstance
	}

	reply.Done = px.my_done

	return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// pick a reasonably close time
	px.ref_time = time.Date(2014, time.May, 10, 0, 0, 0, 0, time.UTC)

	px.instances = make(map[int]Instance)
	px.my_done = -1
	px.done_values = make(map[string]int)
	for _, peer := range px.peers {
		px.done_values[peer] = -1
	}
	px.min_done = -1
	px.seq = 0

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
