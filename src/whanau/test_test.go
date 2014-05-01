package whanau

import "testing"
import "runtime"
import "strconv"
import "os"
import "fmt"
import "math/rand"
import "math"

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
func testGetId(server string, layer int) KeyType {
	args := &GetIdArgs{}
	args.Layer = layer
	var reply GetIdReply
	ok := call(server, "WhanauServer.GetId", args, &reply)
	if ok && (reply.Err == OK) {
		return reply.Key
	}

	return "GETID ERR"
}

/*
func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

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
}

func testRandomWalk(t *testing.T) {
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

	// Testing randomwalk
	fmt.Println("Test: RandomWalk")
	rw1 := testRandomWalk(ws[0].myaddr, 1)
	rw2 := testRandomWalk(ws[0].myaddr, 2)
	fmt.Printf("rand walk 1 from ws0 %s\n", rw1)
	fmt.Printf("rand walk 2 from ws0 %s\n", rw2)
}

func testSampleRecords(t *testing.T) {
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

	// Testing sample record
	fmt.Println("Test: SampleRecord")
	// paxos clusters
	val1 := ValueType{[]string{"s1", "s2"}}
	val2 := ValueType{[]string{"s3", "s4"}}
	val3 := ValueType{[]string{"s5", "s6"}}
	var key1, key2, key3 KeyType = "key1", "key2", "key3"
	ws[0].kvstore[key1] = val1
	ws[0].kvstore[key2] = val2
	ws[0].kvstore[key3] = val3
	testsamples := ws[0].SampleRecords(3)
	fmt.Println("testsamples: ", testsamples)
}

func testGetId(t *testing.T) {
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

	// Testing get id
	fmt.Println("Test: GetId")
	args := &PutIdArgs{}
	args.Layer = 0
	args.Key = "testingGettingKey"
	var reply PutIdReply
	ws[0].PutId(args, &reply)
	testGetId := testGetId(ws[0].myaddr, 0)
	fmt.Println("testgetid: ", testGetId)
}

func testConstructFingers(t *testing.T) {
	runtime.GOMAXPROCS(4)

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

	// hard code in IDs for each server
	for i := 0; i < nservers; i++ {
		ids := make([]KeyType, 0)
		for j := 0; j < L; j++ {
			var id KeyType = KeyType("ws" + strconv.Itoa(i) + "id" + strconv.Itoa(j))
			ids = append(ids, id)
		}
		ws[i].ids = ids
	}
	fmt.Printf("\033[95m%s\033[0m\n", "Test: ConstructFingers Basic")
	fmt.Println("ws[0].ids", ws[0].ids)
	// layer 0
	fingers0 := ws[0].ConstructFingers(0, RF)
	fmt.Println("fingers0:", fingers0)

	// layer 1
	fingers1 := ws[0].ConstructFingers(1, RF)
	fmt.Println("fingers1:", fingers1)
	// layer 2

	fingers2 := ws[0].ConstructFingers(2, RF)
	fmt.Println("fingers2:", fingers2)
}

func testSampleSuccessors(t *testing.T) {
	runtime.GOMAXPROCS(4)

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

	// hard code in dbs for each server
	for i := 0; i < nservers; i++ {
		ws[i].db = make([]Record, RD)
		for j := 0; j < RD; j++ {
			var key KeyType = KeyType("ws" + strconv.Itoa(i) + "key" + strconv.Itoa(j))
			var servers = []string{"server address"}
			var value ValueType = ValueType{servers}
			record := Record{key, value}
			ws[i].db[j] = record
		}
	}
	fmt.Printf("\033[95m%s\033[0m\n", "Test: SuccessorsSample")
	fmt.Println("ws[0].db", ws[0].db)

	args := &SampleSuccessorsArgs{}
	args.Key = "testing sample successors"
	args.T = 1
	var reply SampleSuccessorsReply
	ws[0].SampleSuccessors(args, &reply)
	fmt.Println("testSampleSuccessors: ", reply.Successors)
}

func testSetup(t *testing.T) {
	runtime.GOMAXPROCS(4)

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

	fmt.Printf("\033[95m%s\033[0m\n", "Test: Setup")

	// hard code in records for each server
	for i := 0; i < nservers; i++ {
		for j := 0; j < 3; j++ {
			var key KeyType = KeyType("ws" + strconv.Itoa(i) + "key" + strconv.Itoa(j))
			val := ValueType{}
			for k := 0; k < PaxosSize; k++ {
				val.Servers = append(val.Servers, "ws"+strconv.Itoa(i)+"srv"+strconv.Itoa(k))
			}
			ws[i].kvstore[key] = val
		}

		fmt.Printf("ws[%d].kvstore: ", i)
		fmt.Println(ws[i].kvstore)
	}

	// run setup in parallel
	c := make(chan bool) // writes true of done
	for i := 0; i < nservers; i++ {
		go func(srv int) {
			DPrintf("running ws[%d].Setup", srv)
			ws[srv].Setup(3, 3)
			c <- true
		}(i)
	}

	// wait for all setups to finish
	for i := 0; i < nservers; i++ {
		//time.Sleep(1000)
		done := <-c
		DPrintf("ws[%d] setup done: %b", i, done)
	}

	// check populated ids and fingers

	for i := 0; i < nservers; i++ {
		fmt.Printf("ws[%d].ids: %s\n", i, ws[i].ids)
		fmt.Printf("ws[%d].fingers: %s\n", i, ws[i].fingers)
		fmt.Printf("ws[%d].succ: %s\n\n", i, ws[i].succ)
	}
 }

func testSuccessors(t *testing.T) {
	runtime.GOMAXPROCS(4)

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

	// hard code in IDs for each server
	for i := 0; i < nservers; i++ {
		ids := make([]KeyType, 0)
		for j := 0; j < L; j++ {
			var id KeyType = KeyType("ws" + strconv.Itoa(i) + "id" + strconv.Itoa(j))
			ids = append(ids, id)
		}
		ws[i].ids = ids
	}

	// hard code in dbs for each server
	for i := 0; i < nservers; i++ {
		ws[i].db = make([]Record, RD)
		for j := 0; j < RD; j++ {
			var key KeyType = KeyType("ws" + strconv.Itoa(i) + "key" + strconv.Itoa(j))
			var servers = []string{"server address"}
			var value ValueType = ValueType{servers}
			record := Record{key, value}
			ws[i].db[j] = record
		}
	}

	fmt.Printf("\033[95m%s\033[0m\n", "Test: Successors")
	fmt.Println("ws[0].db", ws[0].db)

	allsuccessors := ws[0].Successors(0)
	fmt.Println("testSuccessors: ", allsuccessors)
}
*/
func TestLookup(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nservers = 10
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

	fmt.Printf("\033[95m%s\033[0m\n", "Test: Lookup")

	const nkeys = 10           // keys are strings from 0 to 99
	const k = nkeys / nservers // keys per node
  keys := make([]KeyType, 0)
	records := make(map[KeyType]ValueType)
	counter := 0
	// hard code in records for each server
	for i := 0; i < nservers; i++ {
		for j := 0; j < nkeys/nservers; j++ {
			//var key KeyType = testKeys[counter]
			var key KeyType = KeyType(strconv.Itoa(counter))
      keys = append(keys, key)
			counter++
			val := ValueType{}
			// randomly pick 5 servers
			for k := 0; k < PaxosSize; k++ {
				val.Servers = append(val.Servers, "ws"+strconv.Itoa(rand.Intn(PaxosSize)))
			}
			records[key] = val
			ws[i].kvstore[key] = val
		}
	}

  for i := 0; i < nservers; i++ {
    fmt.Printf("ws[%d].kvstore: %s\n", i, ws[i].kvstore)
  }
	// run setup in parallel
	// parameters
	constant := 4
	nlayers := constant*int(math.Log(float64(k*nservers))) + 1
	nfingers := constant * int(math.Sqrt(k*nservers))
	w := constant * int(math.Log(float64(nservers))) // number of steps in random walks, O(log n) where n = nservers
  //w := 1
	rd := constant * int(math.Sqrt(k*nservers))            // number of records in the db
	rs := constant * int(math.Sqrt(k*nservers))      // number of nodes to sample to get successors
	ts := constant                                   // number of successors sampled per node

	c := make(chan bool) // writes true of done
	for i := 0; i < nservers; i++ {
		go func(srv int) {
			DPrintf("running ws[%d].Setup", srv)
			ws[srv].Setup(nlayers, nfingers, w, rd, rs, ts)
			c <- true
		}(i)
	}

	// wait for all setups to finish
	for i := 0; i < nservers; i++ {
		done := <-c
		DPrintf("ws[%d] setup done: %b", i, done)
	}

	fmt.Printf("Finished setup\n")

	for i := 0; i < nservers; i++ {
		for j := 0; j < nlayers; j++ {
      fmt.Printf("ws[%d].db: %s\n", i, ws[i].db)
			fmt.Printf("ws[%d].succ[%d]: %s\n\n", i, j, ws[i].succ[j])
		}
	}
  fmt.Printf("Check key coverage in all dbs")

  keyset := make(map[KeyType]bool)
  for i := 0; i < len(keys); i++ {
    keyset[keys[i]] = false
  }

  for i := 0; i < nservers; i++ {
    srv := ws[i]
    for j := 0; j < len(srv.db); j++ {
      keyset[srv.db[j].Key] = true
    }
  }

  // count number of covered keys, all the false keys in keyset
  covered_count := 0 
  for _, v := range keyset {
    if v {
      covered_count++
    }
  }
  fmt.Printf("key coverage in all dbs: %f\n", float64(covered_count)/float64(len(keys)))


  fmt.Printf("Check key coverage in all successor tables")
  keyset = make(map[KeyType]bool)
  for i := 0; i < len(keys); i++ {
    keyset[keys[i]] = false
  }

  for i := 0; i < nservers; i++ {
    srv := ws[i]
    for j := 0; j < len(srv.succ); j++ {
      for k := 0; k < len(srv.succ[j]); k++ {
        keyset[srv.succ[j][k].Key] = true
      }
    }
  }

  // count number of covered keys, all the false keys in keyset
  covered_count = 0 
  for _, v := range keyset {
    if v {
      covered_count++
    }
  }

  fmt.Printf("key coverage in all succ: %f\n", float64(covered_count)/float64(len(keys)))
	// check populated ids and fingers
	/*
		var x0 KeyType = "1"
		var key KeyType = "3"
		finger, layer := ws[0].ChooseFinger(x0, key, nlayers)
		fmt.Printf("chosen finger: %s, chosen layer: %d\n", finger, layer)
 */ 
	
	fmt.Printf("Checking Try for every key from every node\n")
	numFound := 0
	numTotal := 0
	ctr := 0
  fmt.Printf("All test keys: %s\n", keys)
	for i := 0; i < nservers; i++ {
		for j := 0; j < len(keys); j++ {
			key := KeyType(keys[j])
			ctr++
			largs := &LookupArgs{key, nlayers, w, nil}
			lreply := &LookupReply{}
			ws[i].Lookup(largs, lreply)
			if lreply.Err != OK {
        fmt.Printf("Did not find key: %s\n", key)
			} else {
				value := lreply.Value
				fmt.Printf("Found key! returned value: %s, expected value: %s\n", value, records[key])
				// compare string arrays...
				if len(value.Servers) != len(records[key].Servers) {
					t.Fatalf("Wrong value returned (length test): %s expected: %s", value, records[key])
				}
				for k := 0; k < len(value.Servers); k++ {
					if value.Servers[k] != records[key].Servers[k] {
						t.Fatalf("Wrong value returned (length test): %s expected: %s", value, records[key])
					}
				}
				numFound++
			}
			numTotal++
		}
	}

	fmt.Printf("Percent lookups successful: %f\n", float64(numFound)/float64(numTotal))
  
}

