package main

import (
	"os"
	"log"
	"fmt"
	"strconv"
	"sync"
	"errors"
)


/*
  Attributes of this client
*/
var serverConnPort int
var clientId int
//Need a map from clientId->serverConnPort

var clientWaitGroup sync.WaitGroup

func clientPut(clientId int, key string, value string) error {

	/*
	get serverConnPort from clientID from map
	 */

	 if serverConnPort == 0 {
		 return errors.New("Connection does not exist!")
	 }

	/*
	Here goes the code to tell the server to put the key-value pair.
	*/

	// If successful
	return nil
}

func clientGet(clientId int, key string) error {
	/*
	get serverConnPort from clientID from map
	 */

	if serverConnPort == 0 {
		return errors.New("Connection does not exist!")
	}

	/*
	Here goes the code to tell the server to get the value.
	*/

	// If successful
	return nil
}



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
		log.Fatal("Starting client needs three arguments: clientId, serverId and basePort")
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

