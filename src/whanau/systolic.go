package whanau

import "time"

import "fmt"
import "log"

func (ws *WhanauServer) ServerHandler() {
	for !ws.doneMixing {
		args := <-ws.recv_chan

		ws.rec_mu.Lock()

		if _, ok := ws.received_servers[args.Timestep]; ok {
			ws.received_servers[args.Timestep] =
				append(ws.received_servers[args.Timestep], args.Servers)
		} else {
			ws.received_servers[args.Timestep] = make([][]string, 0)
			ws.received_servers[args.Timestep] =
				append(ws.received_servers[args.Timestep], args.Servers)
		}

		ws.rec_mu.Unlock()
	}
}

// RPC to receive random list of servers from neighbors
func (ws *WhanauServer) GetRandomServers(args *SystolicMixingArgs,
	reply *SystolicMixingReply) error {
	// send over the servers to the systolic mixing process
	//fmt.Printf("server %v got getrandom from server %v at ts %d\n",
	//	ws.me, args.SenderAddr, args.Timestep)

	ws.recv_chan <- args

	reply.Err = OK
	return nil
}

// Perform systolic mixing, cf section 9.2 of thesis
func (ws *WhanauServer) PerformSystolicMixing(numWalks int) {
	ws.doneMixing = false
	go ws.ServerHandler()

	server_pool := make([]string, numWalks)
	for i := 0; i < len(server_pool); i++ {
		// populate with own address
		server_pool[i] = ws.myaddr
	}

	naddresses := len(server_pool) / len(ws.neighbors)
	// perform w iterations to get sufficient mixing
	for iter := 0; iter < ws.w; iter++ {
		fmt.Printf("server %v in performsystolic at ts %d\n", ws.me, iter)
		for idx, srv := range ws.neighbors {
			start := idx * naddresses
			end := start + naddresses
			if idx == len(ws.neighbors)-1 {
				end = len(server_pool)
			}
			srv_args := &SystolicMixingArgs{server_pool[start:end], iter + 1,
				ws.myaddr}
			var srv_reply SystolicMixingReply

			ok := call(srv, "WhanauServer.GetRandomServers",
				srv_args, &srv_reply)
			if !ok || srv_reply.Err != OK {
				log.Fatalf("call to server %s failed\n", srv)
				// TODO handle error :(
			}
		}

		// when can we move on? need replies from all neighbors
		val := ws.received_servers[iter+1]

		// val is a list of lists of servers. how long is it?
		// should be as long as the neighbors set.
		for val == nil || len(val) < len(ws.neighbors) {
			time.Sleep(time.Millisecond * 100)
			val = ws.received_servers[iter+1]
		}

		if iter+1 == ws.w {
			// we're done
			// TODO not sure if off by one here
			break
		}

		// free up memory!!
		delete(ws.received_servers, iter)

		// create server pool by concatenating new vals
		server_pool = make([]string, 0)
		for _, v := range val {
			server_pool = append(server_pool, v...)
		}
		server_pool = Shuffle(server_pool)

	}

	// done. After w iterations, we should have a sufficiently randomized
	// list of servers. Save the servers.
	ws.rw_mu.Lock()

	ws.rw_servers = make([]string, len(server_pool))
	copy(ws.rw_servers, server_pool)
	ws.rw_idx = 0

	ws.rw_mu.Unlock()
}
