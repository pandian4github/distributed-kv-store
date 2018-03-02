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
)

/*
  Attributes of this client
*/
var serverId int
var thisClientBasePort int
var thisClientId int
var clientShutDown = false

// client Vector TimeStamp
var thisClientVecTs = shared.Clock{}

type historyValue map[int]shared.Clock

var clientHistory = make(map[string]historyValue)

type ClientMaster int
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

	log.Println("New default server to talk:", serverId, "on base port", clientServerBasePortMap[serverId])

	*status = true
	return nil
}

func (t *ClientMaster) BreakConnection(removeServer *shared.RemoveServerArgs, status *bool) error {
	serverIdToRemove := removeServer.ServerId
	log.Println("Breaking connection between this client", thisClientId, "and server", serverId)
	delete(clientServerBasePortMap, serverIdToRemove)
	delete(clientRpcServerMap, serverIdToRemove)

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

	log.Println("New default server to talk:", serverId, "on port", clientServerBasePortMap[serverId])

	*status = true
	return nil
}

func (t *ClientMaster) KillClient(dummy int, status *bool) error {
	clientShutDown = true
	log.Println("Marked clientShutDown flag..")
	*status = true
	return nil
}

func getClientRpcServer() (*rpc.Client, error) {
	//if server, ok := clientRpcServerMap[serverId]; ok {
	//	return server, nil
	//}
	// servers listen to clients on serverBasePort+2
	serverBasePort := clientServerBasePortMap[serverId]
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

func pruneClientHistory(key string, vecTs shared.Clock) {
	if prevEvents, ok := clientHistory[key]; ok {
		for serverId, ts := range prevEvents {
			if util.HappenedBefore(ts, vecTs) == util.HAPPENED_BEFORE {
				delete(clientHistory[key], serverId)
			}
		}
	}
}

func insertIntoClientHistory(key string, serverId int, vecTs shared.Clock) {
	if _, ok := clientHistory[key]; ok {
		pruneClientHistory(key, vecTs)
		if _, ok := clientHistory[key][serverId]; !ok {
			clientHistory[key][serverId] = vecTs
		}
	} else {
		clientHistory[key] = historyValue{serverId:vecTs}
	}
}

func checkForDependencyError(key string, serverId int, vecTs shared.Clock) bool{
	if concurrentEvents, ok := clientHistory[key]; ok {
		for id, ts := range concurrentEvents {
			if util.TotalOrderOfEvents(vecTs, serverId, ts, id) != util.HAPPENED_AFTER {
				return true
			}
		}
	}
	return false
}


func (t *ClientMaster) ClientPut(args shared.MasterToClientPutArgs, retVal *bool) error {
	incrementMyClientClock()

	key := args.Key
	value := args.Value
	*retVal = false

	if serverId == -1 {
		return errors.New("connection does not exist")
	}

	serverToTalk, err := getClientRpcServer()
	if err != nil {
		return err
	}

	var reply [2]shared.Clock
	putArgs := shared.ClientToServerPutArgs{Key: key, Value: value, ClientId: thisClientId, ClientClock: thisClientVecTs}
	err = serverToTalk.Call("ServerClient.ServerPut", putArgs, &reply)
	serverToTalk.Close()
	if err != nil {
		return err
	} else {
		insertIntoClientHistory(key, serverId, reply[0])
		*retVal = true
	}

	util.UpdateMyClock(&thisClientVecTs, reply[1])
	return nil
}

func incrementMyClientClock() {
	thisClientVecTs[thisClientId]++
}

func (t *ClientMaster) ClientGet(key string, retVal *string) error {
	// Increment clients logical clock on receiving a get request from master
	incrementMyClientClock()

	if serverId == -1 {
		return errors.New("connection does not exist")
	}

	// ServerGet rpc replies with the a value and its vecTs from the key-value store
	reply := new(shared.ServerToClientGetReply)
	reply.Value = shared.Value{}
	reply.ServerVecTs = shared.Clock{}
	serverToTalk, err := getClientRpcServer()
	if err != nil {
		return err
	}
	getArgs := shared.ClientToServerGetArgs{Key:key, ClientVecTs: thisClientVecTs}
	err = serverToTalk.Call("ServerClient.ServerGet", getArgs, &reply)
	serverToTalk.Close()
	if err != nil {
		return err
	}
	util.UpdateMyClock(&thisClientVecTs, reply.ServerVecTs)

	if reply.Value.ClientId == -1 && reply.Value.ServerId == -1 {
		*retVal = "ERR_KEY"
	} else {
		var errDep = false
		errDep = checkForDependencyError(key, reply.Value.ServerId, reply.Value.Ts)
		insertIntoClientHistory(key, serverId, reply.Value.Ts)
		if errDep == true {
			*retVal = "ERR_DEP"
		} else {
			*retVal = reply.Value.Val
		}
	}

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
			util.Sleep(1.0) // so that the RPC call returns before the process is shut down
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

	serverBasePort, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatal("Unable to get the server basePort", serverBasePort)
	}

	thisClientBasePort, err = strconv.Atoi(args[4])
	if err != nil {
		log.Fatal("Unable to get the client basePort", thisClientBasePort)
	}
	incrementMyClientClock()

	log.Println("Starting client with clientId", thisClientId, "serverId", serverId, "serverBasePort", serverBasePort, "clientBasePort", thisClientBasePort)
	clientServerBasePortMap[serverId] = serverBasePort

	//starting two threads
	clientWaitGroup.Add(1)

	go clientListenToMaster()
	clientWaitGroup.Wait()
}