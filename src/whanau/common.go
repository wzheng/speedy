package whanau

const (
	OK          = "OK"
	ErrNoKey    = "ErrNoKey"
	ErrRandWalk = "ErrRandWalk"
)

// for 2PC
const (
	// Action
	Commit = "commit"
	Abort  = "abort"

	// Reply
	Accept = "accept"
	Reject = "reject"

	// Phase
	PhaseOne = "p1"
	PhaseTwo = "p2"
)

type Err string

type KeyType string

type ValueType struct {
	Servers []string
}

type TrueValueType string

// tuple for (id, address) pairs used in finger table
type Pair struct {
	Id      string
	Address string
}

// Key value pair
type Record struct {
	Key   KeyType
	Value ValueType
}

// tuple for (id, address) pairs used in finger table
type Finger struct {
	Id      string
	Address string
}

// Global Parameters
const (
    W = 1  // mixing time of honest region
    RD = 1 // size of db
    RF = 1 // size of fingertable
    RS = 1  // size of succ records
    L = 1 // number of layers
    PaxosSize = 5
    PaxosWalk = 5
)
