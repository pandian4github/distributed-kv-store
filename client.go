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
)


/*
  Attributes of this client
*/
var serverConn int
var serverBasePort int
var clientBasePort int
var clientId int
var clientMasterConnection rpc.Server
//Need a map from clientId->serverConnPort

// client LogicalClock
var clientLogicalClock = 0

type ClientMaster int

type historyKey struct {
	key string
	clientId int
}

type historyValue struct {
	value string
	clientLogicalClock int
}

var clientHistory = make(map[historyKey]historyValue)
var clientRpcServerMap = map[int]*rpc.Client {}

func getClientRpcServer(clientId int) (*rpc.Client, error) {
	if server, ok := clientRpcServerMap[clientId]; ok {
		return server, nil
	}
	// servers listen to clients on serverBasePort+2
	portToConnect := serverBasePort + 2
	hostPortPair := util.LOCALHOST_PREFIX + strconv.Itoa(portToConnect)
	conn, err := util.DialWithRetry(hostPortPair)
	if err != nil {
		return nil, err
	}
	server := rpc.NewClient(conn)
	clientRpcServerMap[clientId] = server
	return server, nil
}

var clientWaitGroup sync.WaitGroup

func (t *ClientMaster) ClientPut(args shared.PutArgs, retVal *bool) error {
	// Increment clients logical clock on receiving a put request from the master
	clientLogicalClock += 1
	key := args.Key
	value := args.Value
	clientId := args.ClientId
	*retVal = false
	// Make a put request to the connected server
	if serverConn == 0 {
		return errors.New("connection does not exist")
	}
 	putArgs := shared.PutArgs{key, value, clientId, serverConn, clientLogicalClock}
 	// reply contains vectorTimeStamp corresponding to this transaction
 	var reply shared.Clock
	serverToTalk, err := getClientRpcServer(clientId)
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
		currKey := historyKey{key, clientId}
		currVal := historyValue{value, reply[clientId]}
		// If some value with same key, clientId pair exists in clientHistory,
		// we can satisfy both READ_YOUR_WRITES or MONOTONIC_READS by just replacing it
		clientHistory[currKey] = currVal
		*retVal = true
	}
	return nil
}

func (t *ClientMaster) ClientGet(args shared.GetArgs, retVal *bool) error {
	// Increment clients logical clock on receiving a get request from master
	clientLogicalClock += 1
	key := args.Key
	clientId := args.ClientId
	*retVal = false
	// Make a get request from the connected server
	if serverConn == 0 {
		return errors.New("connection does not exist")
	}
	getArgs := shared.GetArgs{key, clientId}
	// ServerGet rpc replies with the a value from the key-value store
	reply := new(shared.Value)
	serverToTalk, err := getClientRpcServer(clientId)
	if err != nil {
		return err
	}
	err = serverToTalk.Call("ServerClient.ServerGet", getArgs, &reply)
	if err != nil {
		// ERR_NO_KEY is handled here!
		return err
	} else {
		// Handle ERR_DEP
		// reply contains shared.Value == val, vectorTimeStamp, serverId, clientId
		checkHistoryKey := historyKey{key, clientId}
		checkHistoryValue, ok := clientHistory[checkHistoryKey]
		if ok {
			// Compare reply.Ts[reply.ClientId] to checkHistoryValue.clientLogicalClock
			if reply.Ts[reply.ClientId] < checkHistoryValue.clientLogicalClock {
				fmt.Println("Get Failed: ERR_DEP")
				return errors.New("ERR_DEP")
			} else {
				fmt.Println("Get Successful:", key, "->", reply.Val)
				*retVal = true
			}
		} else {
			// There is no history of reads/writes from this client for key
			fmt.Println("Get Successful:", key, "->", reply.Val)
			*retVal = true
		}
		// Add this transaction to clientHistory
		newHistoryValue := historyValue{reply.Val, reply.Ts[reply.ClientId]}
		clientHistory[checkHistoryKey] = newHistoryValue
	}
	// If successful
	return nil
}

func clientListenToMaster() error {
	defer clientWaitGroup.Done()
	portToListen := clientBasePort
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

	clientId, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Unable to get the clientId", clientId)
	}

	serverConn, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the serverId", serverConn)
	}

	serverBasePort, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatal("Unable to get the server basePort", serverBasePort)
	}

	clientBasePort, err := strconv.Atoi(args[4])
	if err != nil {
		log.Fatal("Unable to get the client basePort", clientBasePort)
	}

	//starting two threads
	clientWaitGroup.Add(2)

	go clientListenToMaster()
	clientWaitGroup.Wait()
}

