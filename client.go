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
	"strings"
)


/*
  Attributes of this client
*/
var serverConn int
var serverBasePort int
var clientBasePort int
var clientId int
var clientServerConnection rpc.Client
var clientMasterConnection rpc.Server
//Need a map from clientId->serverConnPort


type ClientMaster int

type PutArgs struct {
	key, value string
	clientId, serverId int
}

type GetArgs struct {
	key string
}

var clientWaitGroup sync.WaitGroup

func (t *ClientMaster) clientPut(clientId int, key string, value string) error {

	/*
	get serverId from clientId from map in master
	get serverBasePort from serverId from map in master
	 */

	 if serverConn == 0 {
		 return errors.New("Connection does not exist!")
	 }

	 args := &PutArgs{key, value, clientId, serverId}
	 var reply int

	clientServerConnection.Call("ClientServer.serverPut", args, &reply)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Put successful")
	}

	// If successful
	return nil
}

func (t *ClientMaster) clientGet(clientId int, key string) error {

	/*
	get serverId from clientId from map in master
	get serverBasePort from serverId from map in master
	 */

	if serverConn == 0 {
		return errors.New("Connection does not exist!")
	}

	args := &GetArgs{key}
	var reply string

	clientServerConnection.Call("ClientServer.serverGet", args, &reply)

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Put successful")
	}

	// If successful
	return nil
}



func clientListenToMaster() error {
	defer waitGroup.Done()
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
	defer waitGroup.Done()

	portToTalk := serverBasePort+2
	portToTalkStr := "host:"
	portToTalkStr = portToTalkStr + strconv.Itoa(portToTalk)

	clientServerConnection, err := rpc.Dial("tcp", portToTalkStr)
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
		log.Fatal("Unable to get the clientId")
	}

	serverConn, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the serverId")
	}

	serverBasePort, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatal("Unable to get the server basePort")
	}

	clientBasePort, err := strconv.Atoi(args[4])
	if err != nil {
		log.Fatal("Unable to get the client basePort")
	}

	//starting two threads
	clientWaitGroup.Add(2)

	go clientListenToMaster()
	go clientTalkToServer()

	clientWaitGroup.Wait()
}

