package main

import (
	"os"
	"log"
	"strconv"
	"sync"
	"errors"
	"net/rpc"
	"net"
	"./shared"
	"./util"
	//"strings"
	"sort"
	"time"
)


/*
  Attributes of this client
*/
var serverId int
var serverBasePort int
var thisClientBasePort int
var thisClientId int

// client LogicalClock
var clientLogicalClock = 0

var clientShutDown = false

type ClientMaster int

type historyKey struct {
	key string
	clientId int
}

type latestTsDetail struct {
	serverId int
	clock shared.Clock
}

var latestTimestamp = make(map[string]latestTsDetail)
var clientHistory = make(map[historyKey]int)

/*
	Storing connected servers information
 */
var clientServerBasePortMap = map[int]int {}
var clientRpcServerMap = map[int]*rpc.Client {}

func (t *ClientMaster) CreateConnection(server *shared.ClientServerConnectionArgs, status *bool) error {
	serverIdLocal := server.ServerId
	serverBasePortLocal := server.ServerBasePort
	log.Println("Creating connection between this client", thisClientId, "and server", serverIdLocal)

	if _, ok := clientServerBasePortMap[serverIdLocal]; ok {
		*status = true
		return nil
	}

	clientServerBasePortMap[serverIdLocal] = serverBasePortLocal

	/* To get the lowest serverId in the open connections */
	var serverIds []int
	for k := range clientServerBasePortMap {
		serverIds = append(serverIds, k)
	}

	sort.Ints(serverIds)
	if len(serverIds) > 0 {
		serverId = serverIds[0]
	} else {
		serverId = -1
	}

	*status = true
	return nil
}

func (t *ClientMaster) BreakConnection(removeServer *shared.RemoveServerArgs, status *bool) error {
	serverId := removeServer.ServerId
	log.Println("Breaking connection between this client", thisClientId, "and server", serverId)
	delete(clientServerBasePortMap, serverId)
	delete(clientRpcServerMap, serverId)

	/* To get the lowest serverId in the open connections */
	var serverIds []int
	for k := range clientServerBasePortMap {
		serverIds = append(serverIds, k)
	}

	sort.Ints(serverIds)
	if len(serverIds) > 0 {
		serverId = serverIds[0]
	} else {
		serverId = -1
	}

	*status = true
	return nil
}

func (t *ClientMaster) KillClient(dummy int, status *bool) error {
	clientShutDown = true
	log.Println("Marked clientShutDown flag..")
	*status = true
	return nil
}

func getClientRpcServer(serverId int) (*rpc.Client, error) {
	//if server, ok := clientRpcServerMap[serverId]; ok {
	//	return server, nil
	//}
	// servers listen to clients on serverBasePort+2
	serverBasePort = clientServerBasePortMap[serverId]
	portToConnect := serverBasePort + 2
	hostPortPair := util.LOCALHOST_PREFIX + strconv.Itoa(portToConnect)
	conn, err := util.DialWithRetry(hostPortPair)
	if err != nil {
		return nil, err
	}
	server := rpc.NewClient(conn)
	//clientRpcServerMap[serverId] = server
	return server, nil
}

var clientWaitGroup sync.WaitGroup

func (t *ClientMaster) ClientPut(args shared.MasterToClientPutArgs, retVal *bool) error {
	// Increment clients logical clock on receiving a put request from the master
	clientLogicalClock += 1
	key := args.Key
	value := args.Value
	*retVal = false
	log.Println("Received put request for key", key)
	// Make a put request to the connected server

	if serverId == -1 {
		return errors.New("connection does not exist")
	}
	putArgs := shared.ClientToServerPutArgs{key, value, thisClientId, clientLogicalClock}
	// reply contains vectorTimeStamp corresponding to this transaction
	var reply shared.Clock
	serverToTalk, err := getClientRpcServer(serverId)
	if err != nil {
		return err
	}
	err = serverToTalk.Call("ServerClient.ServerPut", putArgs, &reply)
	serverToTalk.Close()
	if err != nil {
		return err
	} else {
		// On a successful put, add this transaction into the client's history
		// reply contains the timestamp recorded at the server for this put call
		log.Println("Put successful")
		currKey := historyKey{key, thisClientId}
		// If some value with same key, clientId pair exists in clientHistory,
		// we can satisfy both READ_YOUR_WRITES or MONOTONIC_READS by just replacing it
		clientHistory[currKey] = clientLogicalClock

		if lts, ok := latestTimestamp[key]; ok {
			if util.TotalOrderOfEvents(lts.clock, lts.serverId, reply, serverId) == util.HAPPENED_BEFORE {
				currLatestTsDetail := latestTsDetail{serverId:serverId, clock:reply}
				latestTimestamp[key] = currLatestTsDetail
			}
		} else {
			latestTimestamp[key] = latestTsDetail{clock:reply, serverId:serverId}
		}
		*retVal = true
	}
	log.Println("End of ClientPut::", clientHistory)
	return nil
}

func (t *ClientMaster) ClientGet(key string, retVal *bool) error {
	// Increment clients logical clock on receiving a get request from master
	clientLogicalClock += 1

	log.Println("Received get request for key", key)
	*retVal = false
	// Make a get request from the connected server

	if serverId == -1 {
		return errors.New("connection does not exist")
	}

	// ServerGet rpc replies with the a value from the key-value store
	reply := new(shared.Value)
	serverToTalk, err := getClientRpcServer(serverId)
	if err != nil {
		return err
	}
	log.Println("Calling ServerGet..")
	err = serverToTalk.Call("ServerClient.ServerGet", key, &reply)
	serverToTalk.Close()
	if err != nil {
		return err
	}

	if reply.ClientId == -1 && reply.ServerId == -1 {
		log.Println("ERR_NO_KEY")
		*retVal = true
		return nil
	}
	// Handle ERR_DEP
	// reply contains shared.Value == val, vectorTimeStamp, serverId, clientId

	//log.Println("clientHistoryValue: ", clientHistory)
	if currLatestTsDetail, ok := latestTimestamp[key]; ok {
		// Get succeeds if the event clearly happened after the event in client history
		// or if the concurrent events are ordered at least at this client
		ordering := util.HappenedBefore(reply.Ts, currLatestTsDetail.clock)
		log.Println("ordering:", ordering)
		if ordering == util.HAPPENED_BEFORE {
			// reply has stale data
			log.Println("Get Failed: ERR_DEP")
			*retVal = true
			return nil
		} else if ordering == util.CONCURRENT {
			if reply.Ts[reply.ClientId] < clientHistory[historyKey{key:key, clientId:reply.ClientId}] {
				// reply has stale data (reply.clientId and thisClientId are same!)
				log.Println("Get Failed: ERR_DEP")
				*retVal = true
				return nil
			}
		}
	}

	// There is no history of reads/writes from this client for key
	latestTimestamp[key] = latestTsDetail{serverId:reply.ServerId, clock:reply.Ts}
	clientHistory[historyKey{key:key, clientId:reply.ClientId}] = reply.Ts[reply.ClientId]
	log.Println("Get Successful:", key, "->", reply.Val)

	*retVal = true
	// Add this transaction to clientHistory
	log.Println("End of ClientGet - clientHistory", clientHistory, "latestTimestamp", latestTimestamp)

	// If successful
	return nil
}

func clientListenToMaster() error {
	defer clientWaitGroup.Done()
	log.Println("Starting thread to listen to master..")
	portToListen := thisClientBasePort
	clientBasePortStr := ":"
	clientBasePortStr = clientBasePortStr + strconv.Itoa(portToListen)

	rpc.Register(new(ClientMaster))

	ln, err := net.Listen("tcp", clientBasePortStr)
	if err != nil {
		log.Fatal(err)
		return err
	}
	for {
		if clientShutDown {
			time.Sleep(time.Second) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to master thread..")
			break
		}
		clientMasterConnection, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
			return err
		}
		rpc.ServeConn(clientMasterConnection)
	}

	return nil
}

func main() {
	args := os.Args
	if len(args) < 4 {
		log.Fatal("Starting client needs four arguments: clientId, serverId, serverConnectionPort and clientBasePort")
	}

	var err error
	thisClientId, err = strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Unable to get the clientId", thisClientId)
	}

	serverId, err = strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the serverId", serverId)
	}

	serverBasePort, err = strconv.Atoi(args[3])
	if err != nil {
		log.Fatal("Unable to get the server basePort", serverBasePort)
	}

	thisClientBasePort, err = strconv.Atoi(args[4])
	if err != nil {
		log.Fatal("Unable to get the client basePort", thisClientBasePort)
	}

	log.Println("Starting client with clientId", thisClientId, "serverId", serverId, "serverBasePort", serverBasePort, "clientBasePort", thisClientBasePort)
	clientServerBasePortMap[serverId] = serverBasePort

	//starting two threads
	clientWaitGroup.Add(1)

	go clientListenToMaster()
	clientWaitGroup.Wait()
}