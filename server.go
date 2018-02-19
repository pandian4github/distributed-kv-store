package main

import (
	"os"
	"log"
	"strconv"
	"strings"
	"errors"
	"sync"
	"github.com/pandian4github/distributed-kv-store/shared"
	"net/rpc"
	"net"
	"time"
)

/*
 Attributes of this server
 Each server listens on three different ports -
   basePort     - listens to master (to connect/disconnect from other servers)
   basePort + 1 - listens to other servers (for gossip and stabilize)
   basePort + 2 - listens to clients (to execute commands)
*/
var thisServerId int
var thisBasePort int

// Maps serverId to host:basePort of that server
var otherServers = map[int]string {}

// To wait for all threads to complete before exiting
var waitGroup sync.WaitGroup

// Flag which is set during a killServer
var shutDown = false

// Core data structures which hold the actual data of the key-value store
var persistedDb = map[string]shared.Value {} // the DB after the last stabilize call
var inFlightDb = map[string]shared.Value {} // the DB which is not yet stabilized with other server nodes

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

/*
Implementation of different RPC methods exported by server to master
*/
type ServerMaster int
func (t *ServerMaster) AddNewServer(newServer *shared.NewServerArgs, status *bool) error {
	newServerId := newServer.ServerId
	hostPortPair := newServer.HostPortPair
	otherServers[newServerId] = hostPortPair
	*status = true
	log.Println("thisServerId:", thisServerId, "Added new server", newServerId)
	return nil
}

func (t *ServerMaster) RemoveServer(removeServer *shared.RemoveServerArgs, status *bool) error {
	serverId := removeServer.ServerId

	if thisServerId == serverId { // Kill this server
		shutDown = true
		log.Println("Marked shutDown flag..")
	} else { // Remove the server details from the map
		log.Println("Removing server", serverId, "from the cluster..")
		delete(otherServers, serverId)
	}
	*status = true
	return nil
}

func (t *ServerMaster) BreakConnection(removeServer *shared.RemoveServerArgs, status *bool) error {
	serverId := removeServer.ServerId
	log.Println("Breaking connection between this server", thisServerId, "and server", serverId)
	delete(otherServers, serverId)
	*status = true
	return nil
}

func (t *ServerMaster) CreateConnection(newServer *shared.NewServerArgs, status *bool) error {
	serverId := newServer.ServerId
	hostPortPair := newServer.HostPortPair
	log.Println("Creating connection between this server", thisServerId, "and server", serverId)
	otherServers[serverId] = hostPortPair
	*status = true
	return nil
}

func (t *ServerMaster) PrintStore(dummy int, status *bool) error {
	var printed = map[string]bool {}
	log.Println("Printing DB contents from server", thisServerId)
	log.Println("{")
	for k, v := range inFlightDb {
		log.Println(k, ":", v.Val)
		printed[k] = true
	}
	for k, v := range persistedDb {
		if !printed[k] {
			log.Println(k, ":", v.Val)
			printed[k] = true
		}
	}
	log.Println("}")
	*status = true
	return nil
}

func (t *ServerMaster) Stabilize(dummy int, status *bool) error {
	log.Println("Stabilize call received from master..")

	/*
	TODO
	Here goes the logic of doing stabilize
	*/

	time.Sleep(time.Second * time.Duration(1 + thisServerId))

	*status = true
	return nil
}

func listenToMaster() error {
	defer waitGroup.Done()
	portToListen := thisBasePort

	serverMaster := new(ServerMaster)
	rpcServer := rpc.NewServer()
	log.Println("Registering ServerMaster..")
	rpcServer.RegisterName("ServerMaster", serverMaster)

	log.Println("Listening to port", portToListen)
	l, e := net.Listen("tcp", "localhost:" + strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer l.Close()

	log.Println("Accepting connections from the listener in address", l.Addr())

	for {
		if shutDown {
			time.Sleep(time.Second) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down the server..")
			break
		}
		log.Println("Listening to connection from the master..")
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
			return err
		}
		//log.Println("Serving the connection request..")
		rpcServer.ServeConn(conn) // synchronous call required?
		//log.Println("Connection request served. ")
	}

	return nil
}


/*
Implementation of different RPC methods exported by server to other servers
*/

func listenToServers() error {
	defer waitGroup.Done()
	//portToListen := basePort + 1

	return nil
}

/*
Implementation of different RPC methods exported by server to clients
*/

func listenToClients() error {
	defer waitGroup.Done()
	//portToListen := basePort + 2

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

	log.Println("Starting server with id", serverId, "to listen on basePort", basePort)
	// If the list of other servers are also specified
	if len(args) >= 4 {
		getOtherServerDetails(args[3])
	}

	log.Println("other server details:", otherServers)
	thisServerId = serverId
	thisBasePort = basePort

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
