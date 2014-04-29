package paxos

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type Instance struct {
	h_prepare int64       // highest prepare seen
	h_accept  int64       // highest number accepted
	h_value   interface{} // value for highest accepted

	decided   bool        // is this instance decided?
	v_decided interface{} // value decided on
}

type PrepareArgs struct {
	Seq         int
	ProposalNum int64
	Done        int
}

type PrepareReply struct {
	OK                  bool
	HighestProposalSeen int64
	HighestValueSeen    interface{}
	Done                int
}

type AcceptArgs struct {
	Seq           int
	ProposalNum   int64
	ValueToAccept interface{}
	Done          int
}

type AcceptReply struct {
	OK   bool // if false, "reject"
	Done int
}

type DecidedArgs struct {
	Seq          int
	ProposalNum  int64
	DecidedValue interface{}
	Done         int
}

type DecidedReply struct {
	OK   bool
	Done int
}
