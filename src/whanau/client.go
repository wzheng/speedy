// Whanau client does not communicate with server on same node;
// rather, client and server are the same machine: server serves
// a certain set of keys and client can make lookup queries.
// Client interfaces with the application to handle failed lookups.

package whanau

import "net/rpc"
import "fmt"

type Clerk struct {
	server string // the "host" server
}

func MakeClerk(server string) *Clerk {
	ck := new(Clerk)
	ck.server = server
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// TODO change to TrueValueType later
func (ck *Clerk) Lookup(key KeyType) ValueType {
	args := &LookupArgs{}
	args.Key = key
	var reply LookupReply
	ok := call(ck.server, "WhanauServer.Lookup", args, &reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		return reply.Value
	}

	return ValueType{}
}

func (ck *Clerk) Get(key KeyType) TrueValueType {
	lookup_args := &LookupArgs{}
	lookup_reply := &LookupReply{}

	lookup_args.NLayers = L
	lookup_args.Steps = W

	ok := call(ck.server, "WhanauServer.Lookup", lookup_args, &lookup_reply)

	if ok && (lookup_reply.Err != ErrNoKey) {
		server_list := lookup_reply.Value
		get_args := &PaxosGetArgs{}
		var get_reply PaxosGetReply

		get_args.Key = key
		get_args.RequestID = NRand()

		for _, server := range server_list.Servers {
			ok := call(server, "WhanauServer.PaxosGetRPC", get_args,
				&get_reply)
			if ok && (get_reply.Err != ErrNoKey) {
				return get_reply.Value
			}
		}
	}

	return ""
}

// TODO this eventually needs to become a real put
// TODO hashing for debugging?
func (ck *Clerk) Put(key KeyType, value TrueValueType) string {
	lookup_args := &LookupArgs{}
	var lookup_reply LookupReply

	ok := call(ck.server, "WhanauServer.Lookup", lookup_args, lookup_reply)

	if ok {
		if lookup_reply.Err == ErrNoKey {
			// TODO: adds the key to its local pending put list
			// TODO: what happens if a client makes a call to insert
			// the same key to 2 different servers? or 2 different clients
			// making 2 different calls to the same key?
			pending_args := &PendingArgs{}
			var pending_reply PendingReply

			add_ok := call(ck.server, "WhanauServer.AddPendingRPC",
				pending_args, pending_reply)

			if !add_ok {
				// TODO error trying to add a pending request...?
				return ""
			}
		} else {
			// TODO: make a paxos request directly to one of the servers
			// TODO error?
			server_list := lookup_reply.Value
			put_args := &PaxosPutArgs{}
			var put_reply PaxosPutReply

			put_args.Key = key
			put_args.Value = value
			put_args.RequestID = NRand()

			for _, server := range server_list.Servers {
				ok := call(server, "WhanauServer.PaxosPutRPC", put_args,
					&put_reply)
				if ok && (put_reply.Err == OK) {
					return ""
				}
			}
		}
	}

	return ""
}
