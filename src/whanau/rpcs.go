package whanau

type LookupArgs struct {
	Key        KeyType
	NLayers    int
	Steps      int
	RoutedFrom []string // servers that have already tried to serve this key
}

type LookupReply struct {
	Err   Err
	Value ValueType
}

type ClientLookupArgs struct {
	Key        KeyType
}

type ClientLookupReply struct {
	Err   Err
	Value TrueValueType
}

// TODO hashing for debugging?
type PutArgs struct {
	Key   KeyType
	Value TrueValueType
}

type PutReply struct {
	Err Err
}

type SampleRecordArgs struct {
}

type SampleRecordReply struct {
	Record Record
	Err    Err
}
type RandomWalkArgs struct {
	Steps int
}

type RandomWalkReply struct {
	// TODO return record?
	Server string
	Err    Err
}

type GetIdArgs struct {
	Layer int
}

type GetIdReply struct {
	Key KeyType
	Err Err
}

type SampleSuccessorsArgs struct {
	Key KeyType
	T   int
}

type SampleSuccessorsReply struct {
	Successors []Record
	Err        Err
}

type QueryArgs struct {
	Key   KeyType
	Layer int
}

type QueryReply struct {
	Value ValueType
	Err   Err
}

type TryArgs struct {
	Key     KeyType
	NLayers int
}

type TryReply struct {
	Value ValueType
	Err   Err
}

type InitPaxosClusterArgs struct {
	RequestServer string
	Phase         string
	Action        string

	// only populated in phase 2
	KeyMap  map[KeyType]TrueValueType
	Servers []string
}

type InitPaxosClusterReply struct {
	Reply string
	Err   Err
}

// Types only used for testing
type PutIdArgs struct {
	Layer int
	Key   KeyType
}

type PutIdReply struct {
	Err Err
}

type PaxosGetArgs struct {
	Key KeyType
}

type PaxosGetReply struct {
	Value TrueValueType
	Err   Err
}

type PaxosPutArgs struct {
	Key   KeyType
	Value TrueValueType
}

type PaxosPutReply struct {
	Err Err
}
