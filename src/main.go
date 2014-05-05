package main

//import "runtime"
import "strconv"
import "os"
import "fmt"
//import "math/rand"
//import "math"
import "time"
import "./whanau"
import "crypto"
import "crypto/rsa"
import "crypto/md5"
import "crypto/rand"

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

/*
func run_dht() {
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
*/

func main() {
  hashMD5 := md5.New()
  hashMD5.Write([]byte("test"))
  Digest := hashMD5.Sum(nil)

  start := time.Now()
  sk, err := rsa.GenerateKey(rand.Reader, 2014);
  elapsed := time.Since(start)
  fmt.Printf("RSA Key Gen time: %s\n", elapsed)

  if err != nil {
    fmt.Println(err);
    return;
  }

  err = sk.Validate();
  if err != nil {
    fmt.Println("Validation failed.", err);
  }

  start = time.Now()
  Sign, Err := rsa.SignPKCS1v15(rand.Reader, sk, crypto.MD5, Digest)
  elapsed = time.Since(start)
  if Err != nil {
    fmt.Println("signing error.", Err)
  }
  fmt.Printf("Signing time: %s\n", elapsed)


  start = time.Now()
  rsa.VerifyPKCS1v15(&sk.PublicKey, crypto.MD5, Digest, Sign)
  elapsed = time.Since(start)
  fmt.Printf("Verifying time: %s\n", elapsed)

  key := whanau.KeyType("testkey")
  val := whanau.ValueType{[]string{"s1", "s2", "s3"}, nil, &sk.PublicKey}

  sig, err1 := whanau.SignValue(key, val, sk)
  val.Sign = sig
  fmt.Println("sig", sig)
  fmt.Println("err1", err1)
  if whanau.VerifyValue(key, val) {
    fmt.Println("value verified!")
  }
  val.Servers = []string{"sybil"}
  if !whanau.VerifyValue(key, val) {
    fmt.Println("value modification detected!")
  }

  sk1, err1 := rsa.GenerateKey(rand.Reader, 2014);
  if err1 != nil {
    fmt.Println(err);
    return;
  }
  val = whanau.ValueType{[]string{"s1", "s2", "s3"}, nil, &sk1.PublicKey}
  val.Sign = sig
  if !whanau.VerifyValue(key, val) {
    fmt.Println("pk modification detected!")
  }
}
