package whanau

type LookupArgs struct {
	Key        string
	RoutedFrom []string // servers that have already tried to serve this key
}

type LookupReply struct {
	Err   Err
	Value string
}

// TODO hashing for debugging?
type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type RandomWalkArgs struct {
  Steps   int
}

type RandomWalkReply struct {
  // TODO return record?
  Server  string
  Err     Err
}


type InitPaxosClusterArgs struct {
	RequestServer string
	Phase string
	Action string
	KeyMap map[string]string
}

type InitPaxosClusterReply struct {
	Reply string
	Err Err
}