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
)


/*
  Attributes of this client
*/
var serverConn int
var serverBasePort int
var clientBasePort int
var clientId int
var clientServerConnection net.Conn
var clientMasterConnection rpc.Server
//Need a map from clientId->serverConnPort

// client LogicalClock
var clientLogicalClock = 0

type ClientMaster int

type PutArgs struct {
	key, value string
	clientId, serverId, clientLogicalClock int
}

type GetArgs struct {
	key string
}

type history struct {
	action string
	key, value string
	clientId, serverId int
	timeStamp map[int]int
}

var clientHistory []history

var clientWaitGroup sync.WaitGroup

func (t *ClientMaster) clientPut(clientId int, key string, value string) error {
	// Increment clients logical clock on receiving a put request from the master
	clientLogicalClock += 1

	// Make a put request to the connected server
	if serverConn == 0 {
		return errors.New("connection does not exist")
	}
 	args := &PutArgs{key, value, clientId, serverConn, clientLogicalClock}
 	// reply contains vectorTimeStamp corresponding to this transaction
 	var reply shared.Clock
	client := rpc.NewClient(clientServerConnection)
	err := client.Call("ServerClient.ServerPut", args, &reply)
	if err != nil {
		fmt.Println(err)
	} else {
		// On a successful put, add this transaction into the client's history
		fmt.Println("Put successful")
		currTransaction := history{"put", key, value, clientId, serverConn, reply}
		clientHistory = append(clientHistory, currTransaction)
	}
	return nil
}

func (t *ClientMaster) clientGet(clientId int, key string) error {

	/*
	get serverId from clientId from map in master
	get serverBasePort from serverId from map in master
	 */

	if serverConn == 0 {
		return errors.New("connection does not exist")
	}

	args := &GetArgs{key}
	var reply string

	client := rpc.NewClient(clientServerConnection)
	err := client.Call("ClientServer.ServerGet", args, &reply)
	defer client.Close()

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Put successful")
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

func clientTalkToServer() error {
	defer clientWaitGroup.Done()

	portToTalk := serverBasePort + 2
	hostPortPair := util.LOCALHOST_PREFIX
	hostPortPair = hostPortPair + strconv.Itoa(portToTalk)

	var err error

	clientServerConnection, err = util.DialWithRetry(hostPortPair)

	if err != nil {
		fmt.Println(err)
		return err
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
	go clientTalkToServer()

	clientWaitGroup.Wait()
}

