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

// Global Parameters
<<<<<<< HEAD
const (
    W = 1  // mixing time of honest region
    RD = 1 // size of db
    RF = 1 // size of fingertable
    RS = 1  // size of succ records
    L = 1 // number of layers
    PaxosSize = 5
    PaxosWalk = 5
)
=======
var W = 1  // mixing time of honest region
var RD = 1 // size of db
var RF = 1 // size of fingertable
var RS = 1 // size of succ records
var L = 1  // number of layers
var PaxosSize = 5
var PaxosWalk = 5
>>>>>>> 9af5fe49ef88b5e123cc673f71f47407f5fa8442
