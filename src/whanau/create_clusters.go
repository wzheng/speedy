package whanau

/*
   Paxos cluster create and join functions
*/

import "fmt"
import "math/rand"

// Master node function
// When a server tells the master node what paxos cluster it's a part
// of, the master node sends it any pending writes that were assigned
// to that server so the server can put it in the paxos cluster.
func (ws *WhanauServer) ReceiveNewPaxosCluster(
	args *ReceiveNewPaxosClusterArgs,
	reply *ReceiveNewPaxosClusterReply) error {
	ws.new_paxos_clusters = append(ws.new_paxos_clusters, args.Cluster)

	// go through the dictionary to see if the master already
	// has some keys for it
	send_keys := make(map[KeyType]TrueValueType)
	//fmt.Printf("looking for server %v\n", args.Server)
	ws.mu.Lock()
	for k, v := range ws.master_paxos_cluster.pending_writes {
		if v == args.Server {
			if value, found := ws.all_pending_writes[k]; found {
				//fmt.Printf("found in pending writes %v\n", value)
				send_keys[k.Key] = value
				// deletes should be safe
				//delete(ws.key_to_server, k)
				delete(ws.all_pending_writes, k)
				//fmt.Printf("send keys is now %v\n", send_keys)
			}
		}
	}
	ws.mu.Unlock()

	reply.KV = send_keys
	reply.Err = OK

	return nil
}

// Join the cluster and add the new keys that this cluster is
// responsible for.
func (ws *WhanauServer) JoinClusterRPC(args *JoinClusterArgs,
	reply *JoinClusterReply) error {

	var wp *WhanauPaxos

	for k, v := range args.KV {

		if p, ok := ws.paxosInstances[k]; ok {
			// WHEEEE :(
			wp = &p
		} else {
			var index int
			for idx, s := range args.NewCluster {
				if s == ws.myaddr {
					index = idx
				}
			}
			
			wp = StartWhanauPaxos(args.NewCluster, index, ws.rpc)
		}

		// initiate paxos call for all of these keys

		// put into one's own kvstore
		ws.mu.Lock()
		ws.kvstore[k] = ValueType{args.NewCluster}
		ws.paxosInstances[k] = *wp
		ws.mu.Unlock()

		cpargs := &ClientPutArgs{k, v, NRand(), ws.myaddr}
		cpreply := &ClientPutReply{}
		ws.PaxosPutRPC(cpargs, cpreply)

		fmt.Printf("Server %v processed %v\n", ws.myaddr, k)
	}

	reply.Err = OK
	return nil
}

func (ws *WhanauServer) InitPaxosCluster(args *InitPaxosClusterArgs, reply *InitPaxosClusterReply) error {
	if args.Phase == PhaseOne {
		reply.Reply = Commit
	} else {
		if args.Action == Commit {
			// first, find the index of itself in the list of servers
			servers := args.Servers
			var index int
			for idx, s := range servers {
				if s == ws.myaddr {
					index = idx
				}
			}

			for k, v := range args.KeyMap {
				ws.mu.Lock()
				ws.kvstore[k] = ValueType{servers}
				fmt.Printf("\ninitiating wp in server %v ... \n\n", ws.me)
				wp := StartWhanauPaxos(servers, index, ws.rpc)
				ws.paxosInstances[k] = *wp
				ws.paxosInstances[k].db[k] = v
				ws.mu.Unlock()
			}
		} else {
			// do nothing?
		}
	}

	return nil
}

func (ws *WhanauServer) ConstructPaxosCluster() []string {

	cluster := make(map[string]bool)

	randIndex := rand.Intn(len(ws.neighbors))
	neighbor := ws.neighbors[randIndex]

	// pick several random walk nodes to join the Paxos cluster
	for i := 0; i < PaxosSize-1; i++ {
		args := &RandomWalkArgs{}
		args.Steps = PaxosWalk
		var reply RandomWalkReply
		ok := call(neighbor, "WhanauServer.RandomWalk", args, &reply)
		if ok && (reply.Err == OK) {

			if _, found := cluster[reply.Server]; found || reply.Server == ws.myaddr {
				i--
				continue
			}

			cluster[reply.Server] = true
		}
	}

	// initiate 2PC with all the nodes in the tentative paxos cluster
	// pass key-value information in the second phase, if it's okay to commit

	var if_commit = true

	for c, _ := range cluster {
		args := &InitPaxosClusterArgs{}
		var reply InitPaxosClusterReply
		args.RequestServer = ws.myaddr
		args.Phase = PhaseOne
		args.Action = ""
		ok := call(c, "WhanauServer.InitPaxosCluster", args, &reply)
		if ok && (reply.Err == OK) {
			if reply.Reply == Reject {
				if_commit = false
				break
			}
		}
	}

	// Send commit message to every server, along with the key
	for c, _ := range cluster {
		args := &InitPaxosClusterArgs{}
		var reply InitPaxosClusterReply
		args.RequestServer = ws.myaddr
		args.Phase = PhaseTwo

		if if_commit {
			args.Action = Commit
		} else {
			args.Action = Abort
		}

		ok := call(c, "WhanauServer.InitPaxosCluster", args, &reply)
		if ok && (reply.Err == OK) {
		}
	}

	var ret []string
	for c, _ := range cluster {
		ret = append(ret, c)
	}

	ret = append(ret, ws.myaddr)

	return ret
}
