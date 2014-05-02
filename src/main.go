package main

import "runtime"
import "strconv"
import "os"
import "fmt"
import "math/rand"
import "math"
import "time"
import "./whanau"

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

func cleanup(ws []*whanau.WhanauServer) {
	for i := 0; i < len(ws); i++ {
		if ws[i] != nil {
			ws[i].Kill()
		}
	}
}

func main() {
	runtime.GOMAXPROCS(4)

	const nservers = 100
	var ws []*whanau.WhanauServer = make([]*whanau.WhanauServer, nservers)
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

		ws[i] = whanau.StartServer(kvh, i, kvh[i], neighbors)
	}

  /*
	var cka [nservers]*whaClerk
	for i := 0; i < nservers; i++ {
		cka[i] = MakeClerk(kvh[i])
	}
  */

	fmt.Printf("\033[95m%s\033[0m\n", "Test: Lookup")

	const nkeys = 100           // keys are strings from 0 to 99
	const k = nkeys / nservers // keys per node
  keys := make([]whanau.KeyType, 0)
	records := make(map[whanau.KeyType]whanau.ValueType)
	counter := 0
	// hard code in records for each server
	for i := 0; i < nservers; i++ {
		for j := 0; j < nkeys/nservers; j++ {
			//var key KeyType = testKeys[counter]
			var key whanau.KeyType = whanau.KeyType(strconv.Itoa(counter))
      keys = append(keys, key)
			counter++
			val := whanau.ValueType{}
			// randomly pick 5 servers
			for kp := 0; kp < whanau.PaxosSize; kp++ {
				val.Servers = append(val.Servers, "ws"+strconv.Itoa(rand.Intn(whanau.PaxosSize)))
			}
			records[key] = val
			//ws[i].kvstore[key] = val
      ws[i].AddToKvstore(key, val)
		}
	}

  /*
  for i := 0; i < nservers; i++ {
    fmt.Printf("ws[%d].kvstore: %s\n", i, ws[i].kvstore)
  }
  */
	// run setup in parallel
	// parameters
	constant := 5
	nlayers := constant*int(math.Log(float64(k*nservers))) + 1
	nfingers := constant * int(math.Sqrt(k*nservers))
	w := constant * int(math.Log(float64(nservers))) // number of steps in random walks, O(log n) where n = nservers
	rd := constant * int(math.Sqrt(k*nservers)) * int(math.Log(float64(nservers)))             // number of records in the db
	rs := constant * int(math.Sqrt(k*nservers)) * int(math.Log(float64(nservers)))      // number of nodes to sample to get successors
	ts := constant                                   // number of successors sampled per node

	c := make(chan bool) // writes true of done
  fmt.Printf("Starting setup\n")
  start := time.Now()
	for i := 0; i < nservers; i++ {
		go func(srv int) {
			whanau.DPrintf("running ws[%d].Setup", srv)
			ws[srv].Setup(nlayers, nfingers, w, rd, rs, ts)
			c <- true
		}(i)
	}

	// wait for all setups to finish
	for i := 0; i < nservers; i++ {
		done := <-c
		whanau.DPrintf("ws[%d] setup done: %b", i, done)
	}

  elapsed := time.Since(start)
  fmt.Printf("Finished setup, time: %s\n", elapsed)

  /*
	for i := 0; i < nservers; i++ {
		for j := 0; j < nlayers; j++ {
      fmt.Printf("ws[%d].db: %s\n", i, ws[i].db)
			fmt.Printf("ws[%d].succ[%d]: %s\n\n", i, j, ws[i].succ[j])
		}
	}
  */
  fmt.Printf("Check key coverage in all dbs")

  keyset := make(map[whanau.KeyType]bool)
  for i := 0; i < len(keys); i++ {
    keyset[keys[i]] = false
  }

  for i := 0; i < nservers; i++ {
    srv := ws[i]
    db := srv.GetDB()
    for j := 0; j < len(db); j++ {
      keyset[db[j].Key] = true
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
  keyset = make(map[whanau.KeyType]bool)
  for i := 0; i < len(keys); i++ {
    keyset[keys[i]] = false
  }

  for i := 0; i < nservers; i++ {
    srv := ws[i]
    succ := srv.GetSucc()
    for j := 0; j < len(succ); j++ {
      for k := 0; k < len(succ[j]); k++ {
        keyset[succ[j][k].Key] = true
      }
    }
  }

  // count number of covered keys, all the false keys in keyset
  covered_count = 0
  missing_keys := make([]whanau.KeyType, 0)
  for k, v := range keyset {
    if v {
      covered_count++
    } else {
      missing_keys = append(missing_keys, k)
    }
  }

  fmt.Printf("key coverage in all succ: %f\n", float64(covered_count)/float64(len(keys)))
  fmt.Printf("missing keys in succs: %s\n", missing_keys)
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
			key := whanau.KeyType(keys[j])
			ctr++
			largs := &whanau.LookupArgs{key, nlayers, w, nil}
			lreply := &whanau.LookupReply{}
			ws[i].Lookup(largs, lreply)
			if lreply.Err != whanau.OK {
        fmt.Printf("Did not find key: %s\n", key)
			} else {
				value := lreply.Value
				// compare string arrays...
				if len(value.Servers) != len(records[key].Servers) {
					//t.Fatalf("Wrong value returned (length test): %s expected: %s", value, records[key])

				  fmt.Printf("Wrong value returned: %s expected: %s\n", value, records[key])
				}
				for k := 0; k < len(value.Servers); k++ {
					if value.Servers[k] != records[key].Servers[k] {
						fmt.Printf("Wrong value returned (length test): %s expected: %s\n", value, records[key])
					}
				}
				numFound++
			}
			numTotal++
		}
	}

	fmt.Printf("Percent lookups successful: %f\n", float64(numFound)/float64(numTotal))
  
}

