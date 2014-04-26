package whanau

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
  ErrRandWalk = "ErrRandWalk"
)

type Err string

// Global Parameters
var W = 1  // mixing time of honest region
var RD = 1 // size of db
var RF = 1 // size of fingertable
var RS = 1  // size of succ records
var L = 1 // number of layers
