package whanau

type LookupArgs struct {
	Key        KeyType
	RoutedFrom []string // servers that have already tried to serve this key
}

type LookupReply struct {
	Err   Err
	Value ValueType
}

// A Put, pending the next Setup.
type PendingArgs struct {
	Key    KeyType
	Value  TrueValueType
	Server string
}

type PendingReply struct {
	Err Err
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
	Key KeyType
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
	Key       KeyType
	RequestID int64
}

type PaxosGetReply struct {
	Value TrueValueType
	Err   Err
}

type PaxosPutArgs struct {
	Key       KeyType
	Value     TrueValueType
	RequestID int64
}

type PaxosPutReply struct {
	Err Err
}

type PaxosPendingInsertsArgs struct {
	Key       KeyType
	View      int
	Server    string
	RequestID int64
}

type PaxosPendingInsertsReply struct {
	Server string
	Err    Err
}

type ClientGetArgs struct {
	Key       KeyType
	RequestID int64
}

type ClientGetReply struct {
	Value string
	Err   Err
}

type ClientPutArgs struct {
	Key        KeyType
	Value      TrueValueType
	RequestID  int64
	Originator string // server where put request originates
}

type ClientPutReply struct {
	Err Err
}

type StartSetupArgs struct {
	MasterServer string
}

type StartSetupReply struct {
	//PendingWrites map[KeyType]ValueType
	Err Err
}

type ReceivePendingWritesArgs struct {
	PendingWrites map[KeyType]TrueValueType
}

type ReceivePendingWritesReply struct {
	Err Err
}

type ReceiveNewPaxosClusterArgs struct {
	Cluster []string
}

type ReceiveNewPaxosClusterReply struct {
	Err Err
}

type WhanauPutRPCArgs struct {
	Key   KeyType
	Value string
}

type WhanauPutRPCReply struct {
	Err Err
}
