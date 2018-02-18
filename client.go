package main

import (
	"os"
	"log"
	"fmt"
	"strconv"
	"sync"
)


/*
  Attributes of this client
*/
var serverConn int
var clientId int
var clientBasePort int

var clientWaitGroup sync.WaitGroup

func clientListenToMaster() error {
	defer waitGroup.Done()
	portToListen := basePort

	return nil
}

func clientTalkToServer() error {
	defer waitGroup.Done()

	return nil
}

func main() {
	args := os.Args
	if len(args) < 4 {
		log.Fatal("Starting server needs three arguments: clientId, serverId and basePort")
	}

	clientId, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Unable to get the clientId")
	}

	serverConn, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the serverId")
	}
	clientBasePort, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the basePort")
	}

	//starting two threads
	clientWaitGroup.Add(2)

	go clientListenToMaster()
	go clientTalkToServer()

	clientWaitGroup.Wait()
}

