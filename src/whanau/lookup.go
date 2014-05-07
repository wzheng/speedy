// Lookup helper methods for Whanau

package whanau

import "math/rand"
import "sort"

// Returns randomly chosen finger and randomly chosen layer as part of lookup
func (ws *WhanauServer) ChooseFinger(x0 KeyType, key KeyType, nlayers int) (Finger, int) {
	// find all fingers from all layers such that the key falls between x0 and the finger id
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
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
					}
				}
			} else {
				// case where x0 > key, compare !(key < x < x0) --> x > x0 or x < key
				if id >= x0 || id <= key {
					if len(candidateFingers) <= counter {
						// only create non empty candidate fingers
						newLayer := make([]Finger, 0)
						candidateFingers = append(candidateFingers, newLayer)
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
						layerMap = append(layerMap, i)
						counter++
					} else {
						candidateFingers[counter] = append(candidateFingers[counter], ws.fingers[i][j])
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
	DPrintf("In Query of server %s", ws.myaddr)
	layer := args.Layer
	key := args.Key
	valueIndex := sort.Search(len(ws.succ[layer]), func(valueIndex int) bool {
		return ws.succ[layer][valueIndex].Key >= key
	})

	if valueIndex < len(ws.succ[layer]) && ws.succ[layer][valueIndex].Key == key {
		DPrintf("In Query: found the key!!!!")
		reply.Value = ws.succ[layer][valueIndex].Value
		DPrintf("reply.Value: %s", reply.Value)
		reply.Err = OK
	} else {
		DPrintf("In Query: did not find key, reply.Value: %s reply.Value.Servers == nil %s", reply.Value, reply.Value.Servers == nil)
		reply.Err = ErrNoKey
	}
	return nil
}

// Try finds the value associated with the key
func (ws *WhanauServer) Try(args *TryArgs, reply *TryReply) error {
	key := args.Key
	nlayers := ws.nlayers
	DPrintf("In Try RPC, trying key: %s", key)
	fingerLength := len(ws.fingers[0])
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
	return nil
}
