package whanau

import "testing"
import "runtime"
import "strconv"
import "os"
import "fmt"
import "math/rand"
import "time"

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "sm-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func cleanup(ws []*WhanauServer) {
	for i := 0; i < len(ws); i++ {
		if ws[i] != nil {
			ws[i].kill()
		}
	}
}

// TODO just for testing
func testRandomWalk(server string, steps int) string {
	args := &RandomWalkArgs{}
	args.Steps = steps
	var reply RandomWalkReply
	ok := call(server, "WhanauServer.RandomWalk", args, &reply)
	if ok && (reply.Err == OK) {
		return reply.Server
	}

	return "RANDOMWALK ERR"
}

// Test getID
func testGetId(server string, layer int) string {
    args := &GetIdArgs{}
    args.Layer = layer
    var reply GetIdReply
    ok := call(server, "WhanauServer.GetId", args, &reply)
    if ok && (reply.Err == OK) {
        return reply.Key
    }

    return "GETID ERR"
}


func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

  rand.Seed(time.Now().UTC().UnixNano()) // for testing
	const nservers = 3
	var ws []*WhanauServer = make([]*WhanauServer, nservers)
	var kvh []string = make([]string, nservers)
	defer cleanup(ws)

	for i := 0; i < nservers; i++ {
		kvh[i] = port("basic", i)
	}

	for i := 0; i < nservers; i++ {
		neighbors := make([]string, 0)
		for j := 0; j < nservers; j++ {
			if j == i {
				continue
			}
			neighbors = append(neighbors, kvh[j])
		}

		ws[i] = StartServer(kvh, i, kvh[i], neighbors)
	}

	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk(kvh[i])
	}

	fmt.Printf("Test: Basic put/lookup ...\n")

	cka[1].Put("a", "x")
	val := cka[1].Lookup("a")

	fmt.Printf("lookup for key a got value %s\n", val)

	fmt.Printf("...Passed\n")

	fmt.Printf("Lookup in neighboring server ...\n")

	cka[2].Put("b", "y")
	val = cka[1].Lookup("b")

	fmt.Printf("lookup for key b got value %s\n", val)

	fmt.Printf("...Passed\n")

  // Testing randomwalk
  rw1 := testRandomWalk(ws[0].myaddr, 1)
  rw2 := testRandomWalk(ws[0].myaddr, 2)
  fmt.Printf("rand walk 1 from ws0 %s\n", rw1)
  fmt.Printf("rand walk 2 from ws0 %s\n", rw2)

  // Testing sample record
  cka[0].Put("testkey", "testval")
  cka[0].Put("testkey1", "testval1")
  cka[0].Put("testkey2", "testval2")
  cka[0].Put("testkey3", "testval3")
  cka[0].Put("testkey4", "testval4")
  cka[0].PutId(0, "testId")
  testsamples := ws[0].SampleRecords(3)
  testGetId := testGetId(ws[0].myaddr, 0)
  fmt.Printf("testsamples: ", testsamples)
  fmt.Printf("testgetid: ", testGetId)
}
