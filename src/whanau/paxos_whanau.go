package whanau

import "math/rand"

func (ws *WhanauServer) InitPaxosCluster(args *InitPaxosClusterArgs, reply *InitPaxosClusterReply) error {
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
