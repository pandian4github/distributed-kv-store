package main

import (
	"os"
	"log"
	"fmt"
	"strconv"
	"sync"
	"errors"
	"net/rpc"
	"net"
	"./shared"
	"./util"
	//"strings"
	"sort"
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

type ClientMaster int

type historyKey struct {
	key string
	clientId int
}

type historyValue struct {
	value string
	vectorTimeStamp shared.Clock
}

var clientHistory = make(map[historyKey]historyValue)

/*
	Storing connected servers information
 */
var clientServerBasePortMap = map[int]int {}
var clientRpcServerMap = map[int]*rpc.Client {}

func (t *ClientMaster) CreateConnection(server *shared.ClientServerConnectionArgs, status *bool) error {
	serverId := server.ServerId
	serverBasePort := server.ServerBasePort
	log.Println("Creating connection between this client", thisClientId, "and server", serverId)

	if _, ok := clientServerBasePortMap[serverId]; ok {
		*status = true
		return nil
	}

	clientServerBasePortMap[serverId] = serverBasePort

	/* To get the lowest serverId in the open connections */
	var serverIds []int
	for k := range clientServerBasePortMap {
		serverIds = append(serverIds, k)
	}

	sort.Ints(serverIds)
	if len(serverIds) > 0 {
		serverId = serverIds[0]
	} else {
		serverId = 0
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
		serverId = 0
	}

	*status = true
	return nil
}

func getClientRpcServer(serverId int) (*rpc.Client, error) {
	if server, ok := clientRpcServerMap[serverId]; ok {
		return server, nil
	}
	// servers listen to clients on serverBasePort+2
	serverBasePort = clientServerBasePortMap[serverId]
	portToConnect := serverBasePort + 2
	hostPortPair := util.LOCALHOST_PREFIX + strconv.Itoa(portToConnect)
	conn, err := util.DialWithRetry(hostPortPair)
	if err != nil {
		return nil, err
	}
	server := rpc.NewClient(conn)
	clientRpcServerMap[serverId] = server
	return server, nil
}

var clientWaitGroup sync.WaitGroup

func (t *ClientMaster) ClientPut(args shared.MasterToClientPutArgs, retVal *bool) error {
	// Increment clients logical clock on receiving a put request from the master
	clientLogicalClock += 1
	key := args.Key
	value := args.Value
	*retVal = false
	// Make a put request to the connected server

	if serverId == 0 {
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
	if err != nil {
		return err
	} else {
		// On a successful put, add this transaction into the client's history
		// reply contains the timestamp recorded at the server for this put call
		fmt.Println("Put successful")
		currKey := historyKey{key, thisClientId}
		currVal := historyValue{value, reply}
		// If some value with same key, clientId pair exists in clientHistory,
		// we can satisfy both READ_YOUR_WRITES or MONOTONIC_READS by just replacing it
		clientHistory[currKey] = currVal
		*retVal = true
	}
	return nil
}

func (t *ClientMaster) ClientGet(key string, retVal *bool) error {
	// Increment clients logical clock on receiving a get request from master
	clientLogicalClock += 1

	*retVal = false
	// Make a get request from the connected server

	if serverId == 0 {
		return errors.New("connection does not exist")
	}

	// ServerGet rpc replies with the a value from the key-value store
	reply := new(shared.Value)
	serverToTalk, err := getClientRpcServer(serverId)
	if err != nil {
		return err
	}
	err = serverToTalk.Call("ServerClient.ServerGet", key, &reply)
	if err != nil {
		// ERR_NO_KEY is handled here!
		return err
	} else {
		// Handle ERR_DEP
		// reply contains shared.Value == val, vectorTimeStamp, serverId, clientId
		checkHistoryKey := historyKey{key, thisClientId}
		checkHistoryValue, ok := clientHistory[checkHistoryKey]
		if ok {
			// Get succeeds if the event clearly happened after the event in client history
			//           or if the concurrent events are ordered at least at this client
			ordering := util.HappenedBefore(reply.Ts, checkHistoryValue.vectorTimeStamp)
			if ordering == util.HAPPENED_BEFORE {
				// reply has stale data
				fmt.Println("Get Failed: ERR_DEP")
				return errors.New("ERR_DEP")
			} else if ordering == util.HAPPENED_AFTER {
				fmt.Println("Get Successful:", key, "->", reply.Val)
				*retVal = true
			} else if ordering == util.CONCURRENT {
				if reply.Ts[reply.ClientId] < checkHistoryValue.vectorTimeStamp[reply.ClientId] {
					// reply has stale data (reply.clientId and thisClientId are same!)
					fmt.Println("Get Failed: ERR_DEP")
					return errors.New("ERR_DEP")
				} else {
					fmt.Println("Get Successful:", key, "->", reply.Val)
					*retVal = true
				}
			}
		} else {
			// There is no history of reads/writes from this client for key
			fmt.Println("Get Successful:", key, "->", reply.Val)
			*retVal = true
		}
		// Add this transaction to clientHistory
		newHistoryValue := historyValue{reply.Val, reply.Ts}
		clientHistory[checkHistoryKey] = newHistoryValue
	}
	// If successful
	return nil
}

func clientListenToMaster() error {
	defer clientWaitGroup.Done()
	portToListen := thisClientBasePort
	clientBasePortStr := ":"
	clientBasePortStr = clientBasePortStr + strconv.Itoa(portToListen)

	rpc.Register(new(ClientMaster))

	ln, err := net.Listen("tcp", clientBasePortStr)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for {
		clientMasterConnection, err := ln.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(clientMasterConnection)
	}

	return nil
}

func main() {
	args := os.Args
	if len(args) < 4 {
		log.Fatal("Starting client needs four arguments: clientId, serverId, serverConnectionPort and clientBasePort")
	}

	thisClientId, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Unable to get the clientId", thisClientId)
	}

	serverId, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the serverId", serverId)
	}

	serverBasePort, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatal("Unable to get the server basePort", serverBasePort)
	}

	clientBasePort, err := strconv.Atoi(args[4])
	if err != nil {
		log.Fatal("Unable to get the client basePort", clientBasePort)
	}

	clientServerBasePortMap[serverId] = serverBasePort

	//starting two threads
	clientWaitGroup.Add(1)

	go clientListenToMaster()
	clientWaitGroup.Wait()
}