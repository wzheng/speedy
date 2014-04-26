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

func (ck *Clerk) Lookup(key KeyType) TrueValueType {
	args := &LookupArgs{}
	args.Key = key
	var reply LookupReply
	ok := call(ck.server, "WhanauServer.Lookup", args, &reply)
	if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
		return reply.Value
	}

	return ""
}

// TODO this eventually needs to become a real put
// TODO hashing for debugging?
func (ck *Clerk) Put(key KeyType, value TrueValueType) string {
	args := &PutArgs{}
	args.Key = key
	args.Value = value
	var reply PutReply
	ok := call(ck.server, "WhanauServer.Put", args, &reply)
	if ok && (reply.Err == OK) {
		return ""
	}

	return ""
}

//Only for testing purposes for testing getid
func (ck *Clerk) PutId(key int, value string) string {
    args := &PutIdArgs{}
    args.Key = key
    args.Value = value
    var reply PutIdReply
    ok := call(ck.server, "WhanauServer.PutId", args, &reply)
    if ok && (reply.Err == OK) {
        return ""
    }
    return ""
}
