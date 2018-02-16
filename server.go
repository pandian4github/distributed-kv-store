package main

import (
	"os"
	"log"
	"strconv"
	"strings"
	"errors"
	"sync"
)

/*
 Attributes of this server
 Each server listens on three different ports -
   basePort     - listens to master (to connect/disconnect from other servers)
   basePort + 1 - listens to other servers (for gossip and stabilize)
   basePort + 2 - listens to clients (to execute commands)
*/
var serverId int
var basePort int

// Maps serverId to host:basePort of that server
var otherServers = map[int]string {}

// To wait for all threads to complete before exiting
var waitGroup sync.WaitGroup

// From the given command line argument, parse the other server details
// Format: serverId1@host1:basePort1,serverId2@host2:basePort2,...
func getOtherServerDetails(str string) error {
	servers := strings.Split(str, ",")
	for _, server := range servers {
		if server == "" {
			continue
		}
		parts := strings.Split(server, "@")
		if len(parts) != 2 {
			return errors.New("serverDetail should contain two parts - serverId@host:basePort")
		}
		serverId, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}
		hostPortPair := parts[1]
		otherServers[serverId] = hostPortPair
	}
	return nil
}

func listenToMaster() error {
	defer waitGroup.Done()
	portToListen := basePort

	return nil
}

func listenToServers() error {
	defer waitGroup.Done()
	portToListen := basePort + 1

	return nil
}

func listenToClients() error {
	defer waitGroup.Done()
	portToListen := basePort + 2

	return nil
}

func talkToServers() error {
	defer waitGroup.Done()

	return nil
}

/*func print(serverId int, basePort int, threadId int) {
	defer waitGroup.Done()
	for i := 0; i < 1000; i++ {
		log.Println(threadId, serverId, basePort)
		time.Sleep(time.Second)
	}
}*/

func main() {
	args := os.Args
	if len(args) < 3 {
		log.Fatal("Starting server needs at least two arguments: serverId and basePort")
	}

	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		log.Fatal("Unable to get the serverId")
	}
	basePort, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatal("Unable to get the basePort")
	}

	// If the list of other servers are also specified
	if len(args) >= 4 {
		getOtherServerDetails(args[3])
	}

	//log.Println(otherServers)

	// since we are starting three threads
	waitGroup.Add(4)

	go listenToMaster()
	go listenToServers()
	go listenToClients()
	go talkToServers()

/*	for i := 0; i < 10; i++ {
		go print(serverId, basePort, i)
		waitGroup.Add(1)
	}
*/
	waitGroup.Wait()
}
