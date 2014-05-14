// Lookup helper methods for Whanau

package whanau

import "math/rand"
import "sort"

//import "time"
//import "fmt"

// Returns randomly chosen finger and randomly chosen layer as part of lookup
func (ws *WhanauServer) ChooseFinger(x0 KeyType, key KeyType, nlayers int) (Finger, int) {
	// find all fingers from all layers such that the key falls
	// between x0 and the finger id
	candidateFingers := make([][]Finger, 0)
	// maps index to nonempty layer number
	layerMap := make([]int, 0)
	counter := 0
	for i := 0; i < nlayers; i++ {
		DPrintf("ws.fingers[%d]: %s", i, ws.fingers[i])
		for j := 0; j < len(ws.fingers[i]); j++ {

			// compare x0 <= id <= key on a circle
			id := ws.fingers[i][j].Id
			if x0 <= key {
				if x0 <= id && id <= key {
					if len(candidateFingers) <= counter {
						// only create non empty candidate fingers
						newLayer := make([]Finger, 0)
						candidateFingers = append(candidateFingers, newLayer)
						candidateFingers[counter] = append(
							candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(
							candidateFingers[counter], ws.fingers[i][j])
					}
				}
			} else {
				// case where x0 > key, compare !(key < x < x0) --> x > x0 or x < key
				if id >= x0 || id <= key {
					if len(candidateFingers) <= counter {
						// only create non empty candidate fingers
						newLayer := make([]Finger, 0)
						candidateFingers = append(candidateFingers, newLayer)
						candidateFingers[counter] = append(
							candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(
							candidateFingers[counter], ws.fingers[i][j])
					}
				}
			}
		}
	}

	DPrintf("len(candidateFingers): %d, len(layerMap): %d", len(candidateFingers), len(layerMap))
	// pick random layer out of nonempty candidate fingers
	if len(candidateFingers) > 0 {
		randIndex := rand.Intn(len(candidateFingers))
		finger := candidateFingers[randIndex][rand.Intn(len(candidateFingers[randIndex]))]
		return finger, layerMap[randIndex]
	}

	// if can't find any, randomly choose layer and randomly return finger
	// TODO probably shouldn't get here?
	randLayer := rand.Intn(len(ws.fingers))
	randfinger := ws.fingers[randLayer][rand.Intn(len(ws.fingers[randLayer]))]
	return randfinger, randLayer
}

// Query for a key in the successor table
func (ws *WhanauServer) Query(args *QueryArgs, reply *QueryReply) error {
	var queryReply QueryReply
	if !ws.is_sybil {
		layer := args.Layer
		key := args.Key
		queryReply = ws.HonestQuery(key, layer)
	} else {
		queryReply = ws.SybilQuery()
	}
	reply.Value = queryReply.Value
	reply.Err = queryReply.Err
	//fmt.Printf("Ending query search: %s", ws.myaddr)
	return nil
}

// Honest query
func (ws *WhanauServer) HonestQuery(key KeyType, layer int) QueryReply {
	var reply QueryReply
	//fmt.Printf("Starting binary search: %s", ws.myaddr)
	var valueIndex int
	if layer < len(ws.succ) {
		valueIndex = sort.Search(len(ws.succ[layer]), func(valueIndex int) bool {
			return ws.succ[layer][valueIndex].Key >= key
		})
	} else {
		valueIndex = -1
	}
	//fmt.Printf("Ending binary search: %s", ws.myaddr)
	if valueIndex != -1 && valueIndex < len(ws.succ[layer]) && valueIndex < len(ws.succ[layer]) && ws.succ[layer][valueIndex].Key == key {
		DPrintf("In Query: found the key!!!! %v\n", key)
		reply.Value = ws.succ[layer][valueIndex].Value
		DPrintf("reply.Value: %s\n", reply.Value)
		reply.Err = OK
	} else {
		DPrintf("did not find key\n")
		reply.Err = ErrNoKey
	}
	//fmt.Printf("Ending query search: %s", ws.myaddr)
	return reply
}

// Sybil query
func (ws *WhanauServer) SybilQuery() QueryReply {
	var queryReply QueryReply
	queryReply.Err = ErrNoKey
	return queryReply
}

// Try finds the value associated with the key
func (ws *WhanauServer) Try(args *TryArgs, reply *TryReply) error {
	var tryReply TryReply
	if !ws.is_sybil {
		key := args.Key
		tryReply = ws.HonestTry(key)
	} else {
		tryReply = ws.SybilTry()
	}
	reply.Value = tryReply.Value
	reply.Err = tryReply.Err
	return nil
}

// Try finds the value associated with the key in honest node
func (ws *WhanauServer) HonestTry(key KeyType) TryReply {
	nlayers := ws.nlayers
	DPrintf("In %s Honest Try RPC, trying key: %s", ws.myaddr, key)

	var reply TryReply

	// Lookup in local kvstore (pg 60 of thesis)
	if val, ok := ws.kvstore[key]; ok {
		DPrintf("local look up found %s\n", key)
		reply.Value = val
		reply.Err = OK
		return reply
	}

	var fingerLength int
  DPrintf("ws.fingers: %s", ws.fingers)
	if len(ws.fingers) > 0 {
		fingerLength = len(ws.fingers[0])
		j := sort.Search(fingerLength, func(i int) bool {
			return ws.fingers[0][i].Id >= key
		})
		j = j % fingerLength
		if j < 0 {
			j = j + fingerLength
		}
		j = (j + fingerLength - 1) % fingerLength
		count := 0
		queryArgs := &QueryArgs{}
		queryReply := &QueryReply{}
		for queryReply.Err != OK && count < TIMEOUT {
			f, i := ws.ChooseFinger(ws.fingers[0][j].Id, key, nlayers)
			queryArgs.Key = key
			queryArgs.Layer = i
			call(f.Address, "WhanauServer.Query", queryArgs, queryReply)
			j = j - 1
			j = j % fingerLength
			if j < 0 {
				j = j + fingerLength
			}

			count++
		}

		if queryReply.Err == OK {
			DPrintf("Found key in Try!")
			value := queryReply.Value
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrNoKey
	}

	return reply
}

// Helper method for sybil try
func (ws *WhanauServer) SybilTry() TryReply {
	var tryReply TryReply
	tryReply.Err = ErrNoKey
	return tryReply
}

// Returns paxos cluster for given key.
// Called by servers to figure out which paxos cluster
// to get the true value from.
func (ws *WhanauServer) Lookup(args *LookupArgs, reply *LookupReply) error {
	var lookupReply LookupReply
	if !ws.is_sybil {
		key := args.Key
		steps := ws.w
		lookupReply = ws.HonestLookup(key, steps)
	} else {
		lookupReply = ws.SybilLookup()
	}
	reply.Value = lookupReply.Value
	reply.Err = lookupReply.Err
	//fmt.Printf("Lookup returned %v\n", reply.Value)
	return nil
}

// Helper method for honest lookup
func (ws *WhanauServer) HonestLookup(key KeyType, steps int) LookupReply {
	DPrintf("In Lookup key: %s server %s", key, ws.myaddr)
	reply := LookupReply{}

	addr := ws.myaddr
	count := 0

	tryArgs := &TryArgs{key}
	tryReply := &TryReply{}

	for tryReply.Err != OK && count < TIMEOUT {
		call(addr, "WhanauServer.Try", tryArgs, tryReply)
		/*randomWalkArgs := &RandomWalkArgs{steps}
		randomWalkReply := &RandomWalkReply{}
		call(ws.myaddr, "WhanauServer.RandomWalk", randomWalkArgs, randomWalkReply)
		if randomWalkReply.Err == OK {
			addr = randomWalkReply.Server
		}*/

		// Get a server from the lookup cache reserve
		addr, _ = ws.GetLookupServer()
		count++
	}

	if tryReply.Err == OK {
		value := tryReply.Value
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return reply
}

// Helper method for sybil lookup
func (ws *WhanauServer) SybilLookup() LookupReply {
	reply := LookupReply{}
	reply.Err = ErrNoKey
	return reply
}

// return random Key/value record from local storage
func (ws *WhanauServer) SampleRecord(args *SampleRecordArgs, reply *SampleRecordReply) error {
	var samplereply SampleRecordReply
	if ws.is_sybil {
		samplereply = ws.SybilSampleRecord()
	} else {
		samplereply = ws.HonestSampleRecord()
	}
	reply.Record = samplereply.Record
	reply.Err = samplereply.Err
	return nil
}

// honest node samplerecord
func (ws *WhanauServer) HonestSampleRecord() SampleRecordReply {
  if len(ws.kvstore) < 1 {
    var sampleRecordReply SampleRecordReply
    sampleRecordReply.Err = ErrNoKey
    return sampleRecordReply
  }
	randIndex := rand.Intn(len(ws.kvstore))
	keys := make([]KeyType, 0)
	for k, _ := range ws.kvstore {
		keys = append(keys, k)
	}
	key := keys[randIndex]
	value := ws.kvstore[key]
	record := Record{key, value}
	return SampleRecordReply{record, OK}
}

// sybil node samplerecord
func (ws *WhanauServer) SybilSampleRecord() SampleRecordReply {
	key := KeyType("This is a Sybil key")
	value := make([]string, 0)
	value = append(value, "HA")
	record := Record{key, ValueType{value}}

	if len(ws.kvstore) > 0 {
		for k, _ := range ws.kvstore {
			key = k
			break
		}
		val := ws.kvstore[key]
		record = Record{key, val}
	}

	return SampleRecordReply{record, OK}
}

// Returns a list of records sampled randomly from local kv store
// Note: we agreed that duplicates are fine
func (ws *WhanauServer) SampleRecords(rd int, steps int) []Record {

	records := make([]Record, 0)
	for i := 0; i < rd; i++ {
		// random walk
		rwargs := &RandomWalkArgs{steps}
		rwreply := &RandomWalkReply{}
		counter := 0
		for rwreply.Err != OK && counter < TIMEOUT {
			ws.RandomWalk(rwargs, rwreply)
			counter++
		}
		server := rwreply.Server
		// Do rpc call to samplerecord
		srargs := &SampleRecordArgs{}
		srreply := &SampleRecordReply{}
		counter = 0
		for srreply.Err != OK && counter < TIMEOUT {
			call(server, "WhanauServer.SampleRecord", srargs, srreply)
      counter++
		}

		if srreply.Err == OK {
			records = append(records, srreply.Record)
		}
	}
	return records
}

// Constructs Finger table for a specified layer
func (ws *WhanauServer) ConstructFingers(layer int) []Finger {
	//start := time.Now()
	//defer fmt.Printf("CONSTRUCTFINGERS in server %v took %v\n",
	//ws.myaddr, time.Since(start))

	DPrintf("In ConstructFingers of %s, layer %d", ws.myaddr, layer)
	fingers := make([]Finger, 0, ws.rf*2)
	for i := 0; i < ws.rf; i++ {
		args := &RandomWalkArgs{ws.w}
		reply := &RandomWalkReply{}

		// Keep trying until succeed or timeout
		counter := 0
		for reply.Err != OK && counter < TIMEOUT {
			ws.RandomWalk(args, reply)
			counter++
		}

		server := reply.Server

		// get id of server using rpc call to that server
		getIdArg := &GetIdArgs{layer}
		getIdReply := &GetIdReply{}
		ok := false

		// block until succeeds
    counter = 0
		for (!ok || (getIdReply.Err != OK)) && counter < TIMEOUT {
			DPrintf("rpc to getid of %s from ConstructFingers %s layer %d", server, ws.myaddr, layer)
			ok = call(server, "WhanauServer.GetId", getIdArg, getIdReply)
			counter++
		}

		if getIdReply.Err == OK {
			finger := Finger{getIdReply.Key, server}
			fingers = append(fingers, finger)
		}
	}

	return fingers
}

// Choose id for specified layer
func (ws *WhanauServer) ChooseID(layer int) KeyType {
	//fmt.Printf("Currently choosing id: %s \n", ws.myaddr)
	if ws.is_sybil {
		return ws.SybilChooseID(layer)
	} else {
		return ws.HonestChooseID(layer)
	}
}

// Honest choose id
func (ws *WhanauServer) HonestChooseID(layer int) KeyType {
	//fmt.Printf("In ChooseID of honest %s, layer %d \n", ws.myaddr, layer)
  if len(ws.db) < 1 {
    return ErrNoKey
  }

	if layer == 0 {
		// choose randomly from db
		randIndex := rand.Intn(len(ws.db))
		record := ws.db[randIndex]
		DPrintf("record.Key", record.Key)
		return record.Key

	} else {
		// choose finger randomly from layer - 1, use id of that finger
    if len(ws.fingers[layer-1]) == 0 {
      return ErrNoKey
    }
		randFinger := ws.fingers[layer-1][rand.Intn(len(ws.fingers[layer-1]))]
		return randFinger.Id
	}
}

// Sybil choose id
func (ws *WhanauServer) SybilChooseID(layer int) KeyType {
	//fmt.Printf("In ChooseID Sybil %s, layer %d \n", ws.myaddr, layer)
	key := KeyType("Sybil Node ID")
	for k := range ws.kvstore {
		key = k
	}
	return key
}

// Gets successors that are nearest each key
func (ws *WhanauServer) SampleSuccessors(args *SampleSuccessorsArgs, reply *SampleSuccessorsReply) error {
	//start := time.Now()
	//defer fmt.Printf("SAMPLESUCCESSORS in server %v took %v\n",
	//	ws.myaddr, time.Since(start))

	key := args.Key
	records := make([]Record, ws.t*2)
	//fmt.Printf("Sampling successors: %s \n", ws.myaddr)
	if ws.t <= len(ws.db) {
		firstRecord := PositionOf(key, ws.db)
		remaining := len(ws.db) - firstRecord
		if remaining >= ws.t {
			copy(records, ws.db[firstRecord:firstRecord+ws.t])
		} else {
			headIdx := ws.t - remaining
			copy(records, ws.db[firstRecord:])
			copy(records, ws.db[:headIdx])
		}
		reply.Successors = records
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (ws *WhanauServer) Successors(layer int) []Record {
	//start := time.Now()
	//defer fmt.Printf("SUCCESSORS in server %v took %v\n",
	//	ws.myaddr, time.Since(start))

	//fmt.Printf("In Sucessors of %s, layer %d \n", ws.myaddr, layer)
  if layer >= len(ws.ids) || len(ws.ids[layer]) < 1 {
    return make([]Record, 0)
  }

	// overallocate memory for array
	successors := make([]Record, 0, ws.rs*ws.t*2)
	maxIteration := 50
	counter := 0
	for i := 0; i < ws.rs; i++ {
		args := &RandomWalkArgs{}
		args.Steps = ws.w
		reply := &RandomWalkReply{}
		ws.RandomWalk(args, reply)

		if reply.Err == OK {
			vj := reply.Server

			//fmt.Printf("random walk reply: %s \n", vj)
			sampleSuccessorsArgs := &SampleSuccessorsArgs{ws.ids[layer]}
			sampleSuccessorsReply := &SampleSuccessorsReply{}
			for sampleSuccessorsReply.Err != OK && counter < maxIteration {
				counter++
				call(vj, "WhanauServer.SampleSuccessors",
					sampleSuccessorsArgs, sampleSuccessorsReply)
			}

			if sampleSuccessorsReply.Err != OK {
				sampleSuccessorsReply.Successors = make([]Record, 0)
				sampleSuccessorsReply.Err = OK
			}
			successors = append(successors,
				sampleSuccessorsReply.Successors...)
		}
	}
	//fmt.Printf("These are the successors: %s \n", successors)
	return successors
}
