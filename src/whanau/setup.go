package whanau

/*
 Functions related to Whanau setup.
*/

import "time"
import "fmt"
import "math/rand"

func (ws *WhanauServer) Setup() {
	//fmt.Printf("In setup of honest node: %s", ws.is_sybil)
	if ws.is_sybil {
		ws.SetupSybil()
	} else {
		//fmt.Printf("Setting up honest server: %s", "honest server REALLY")
		ws.SetupHonest()
	}
}

// Setup for honest nodes
func (ws *WhanauServer) SetupHonest() {
	//DPrintf("In Setup of honest server %s", ws.myaddr)
	DPrintf("HONEST SERVER: %s", "HONEST SERVER")
	// fill up db by randomly sampling records from random walks
	// "The db table has the good property that each honest node’s stored records are frequently represented in other honest nodes’db tables"
	ws.db = ws.SampleRecords(ws.rd, ws.w)
	// TODO probably don't need lock?
	By(RecordKey).Sort(ws.db)

	// reset ids, fingers, succ
	ws.ids = make([]KeyType, 0)
	ws.fingers = make([][]Finger, 0)
	ws.succ = make([][]Record, 0)
	for i := 0; i < ws.nlayers; i++ {
		// populate tables in layers
		//fmt.Printf("Choosing ID: %s", ws.ChooseID(i))
		ws.ids = append(ws.ids, ws.ChooseID(i))
		//fmt.Printf("Choosing Fingers: %s", ws.ConstructFingers(i))
		curFingerTable := ws.ConstructFingers(i)
		ByFinger(FingerId).Sort(curFingerTable)
		ws.fingers = append(ws.fingers, curFingerTable)

		//fmt.Printf("Choosing successors: %s", ws.Successors(i))
		curSuccessorTable := ws.Successors(i)
		By(RecordKey).Sort(curSuccessorTable)
		ws.succ = append(ws.succ, curSuccessorTable)

	}
	//fmt.Printf("Server ids: %s", ws.ids)
	//fmt.Printf("Server fingers: %s", ws.fingers)
	//fmt.Printf("Server successors: %s", ws.succ)
}

// Server for Sybil nodes
func (ws *WhanauServer) SetupSybil() {
	DPrintf("In Setup of Sybil server %s", ws.myaddr)

	// reset ids, fingers, succ...etc.
	ws.db = make([]Record, 0)
	ws.ids = make([]KeyType, 0)
	ws.fingers = make([][]Finger, 0)
	ws.succ = make([][]Record, 0)
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

				}
			}
		}

		time.Sleep(1800 * time.Second)
	}
}

// each server changes its current state, and sends the setup messages
// to all of its neighbors
func (ws *WhanauServer) StartSetup(args *StartSetupArgs, reply *StartSetupReply) error {

	ws.mu.Lock()
	if ws.state == Normal {
		ws.state = PreSetup
		go ws.StartSetupStage2()
	} else {
		ws.mu.Unlock()
		reply.Err = OK
		return nil
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

	reply.Err = OK

	return nil
}

func (ws *WhanauServer) StartSetupStage2() {

	// wait until all of its current outstanding requests are done processing
	// TODO: should keep a counter of all of the outstanding requests

	// try to construct a new paxos cluster
	new_cluster := ws.ConstructPaxosCluster()
	// send this new cluster to a random master cluster
	fmt.Printf("Server %v DONE constructing new paxos clusters, new cluster is %v\n", ws.myaddr, new_cluster)

	// enter the SETUP stage
	ws.mu.Lock()
	ws.state = Setup
	ws.mu.Unlock()

	// TODO: for every key value in the current kv store, replace with the newest paxos cluster

	randInt := rand.Intn(len(ws.masters))
	master_server := ws.masters[randInt]

	receive_paxos_args := &ReceiveNewPaxosClusterArgs{ws.myaddr, new_cluster}
	receive_paxos_reply := &ReceiveNewPaxosClusterReply{}

	ok := call(master_server, "WhanauServer.ReceiveNewPaxosCluster",
		receive_paxos_args, receive_paxos_reply)
	if ok {
		if receive_paxos_reply.Err == OK {

			fmt.Printf("Server %v received pending write %v\n", ws.myaddr, receive_paxos_reply.KV)

			join_args := JoinClusterArgs{new_cluster, receive_paxos_reply.KV}
			var join_reply JoinClusterReply

			for _, srv := range new_cluster {
				if srv == ws.myaddr {
					ws.JoinClusterRPC(&join_args, &join_reply)
				} else {
					ok := call(srv, "WhanauServer.JoinClusterRPC",
						join_args, &join_reply)
					if !ok {
						// TODO error
					}
				}
			}

		}
	}

	// move on to the next stage
	ws.mu.Lock()
	ws.state = WhanauSetup
	ws.mu.Unlock()

	c := make(chan bool) // writes true of done
	go func() {
		ws.Setup()
		c <- true
	}()

	<-c

	ws.mu.Lock()
	ws.state = Normal
	ws.mu.Unlock()

	fmt.Printf("Server %v finished entire setup stage\n", ws.myaddr)
}
