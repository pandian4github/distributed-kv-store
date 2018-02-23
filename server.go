package main

import (
	"os"
	"log"
	"strconv"
	"strings"
	"errors"
	"sync"
	"./shared"
	"net/rpc"
	"net"
	"time"
	"./util"
	"math/rand"
	//"fmt"
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

var thisVecTs = shared.Clock {}

// Maps serverId to host:basePort of that server
var otherServers = map[int]string {}

// Maintains a map of open connections to the other servers
var serverRpcClientMap = map[int]*rpc.Client {}

// To wait for all threads to complete before exiting
var waitGroup sync.WaitGroup

// Flag which is set during a killServer
var shutDown = false

// Flag to check if data is received from all other servers during a stabilize call
var dataReceivedFrom = map[int]shared.StabilizeDataPacket {}
var dbReceivedFrom = map[int]DB {}
var isDataReceivedFrom = map[int]int {}
var numServersReceivedFrom = 0
var allConnections = map[int]map[int]string{}
var receivedFromAllServers bool
var propagatedToServer = map[int]map[int]int {} // {x: y} to denote if y's data is already propagated to x
var clockDuringStabilize = shared.Clock {}

// Core data structures which hold the actual data of the key-value store
type DB map[string]shared.Value

var persistedDb = DB {} // the DB after the last stabilize call
var inFlightDb = DB {} // the DB which is not yet stabilized with other server nodes

func getServerRpcClient(serverId int) (*rpc.Client, error) {
	if client, ok := serverRpcClientMap[serverId]; ok {
		return client, nil
	}
	if hostBasePortPair, ok := otherServers[serverId]; ok {
		basePortStr := strings.Split(hostBasePortPair,  ":")[1]
		basePort, err := strconv.Atoi(basePortStr)
		if err != nil {
			return nil, err
		}
		portToConnect := basePort + 1
		hostPortPair := util.LOCALHOST_PREFIX + strconv.Itoa(portToConnect)
		conn, err := util.DialWithRetry(hostPortPair)
		if err != nil {
			return nil, err
		}
		client := rpc.NewClient(conn)
		serverRpcClientMap[serverId] = client
		return client, nil
	} else {
		return nil, errors.New("port information not found for the server " + strconv.Itoa(serverId))
	}
}

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

func collateAndResolveConflicts() error {
	// Updating persistedDb after resolving conflicts from the other server's updates
	for _, partialDB := range dbReceivedFrom {
		for key, currValue := range partialDB {
			prevValue, ok := persistedDb[key]
			if ok {
				ordering := util.TotalOrderOfEvents(prevValue.Ts, prevValue.ServerId, currValue.Ts, currValue.ServerId)
				if ordering == util.HAPPENED_BEFORE {
					persistedDb[key] = currValue
				}
			} else {
				persistedDb[key] = currValue
			}
		}
	}
	// Updating persistedDb after resolving conflicts from the updates to this server
	for key, currValue := range inFlightDb {
		prevValue, ok := persistedDb[key]
		if ok {
			ordering := util.TotalOrderOfEvents(prevValue.Ts, prevValue.ServerId, currValue.Ts, currValue.ServerId)
			if ordering == util.HAPPENED_BEFORE {
				persistedDb[key] = currValue
			}
		} else {
			persistedDb[key] = currValue
		}
	}
	// After all the updates in flight are persisted clear the inFlightDB
	inFlightDb = make(DB)
	return nil
}

func sendMyDataToNeighbors() error {
	for serverId := range otherServers {
		client, err := getServerRpcClient(serverId)
		if err != nil {
			return err
		}

		log.Println("Sending inFlightData to server", serverId)
		var thisServerDataPacket = shared.StabilizeDataPacket{InFlightDB:inFlightDb, VecTs:thisVecTs, Peers:otherServers}
		var args = shared.StabilizeDataPackets{thisServerId:thisServerDataPacket}

		var reply bool
		client.Call("ServerServer.SendDataPackets", args, &reply)
		if reply {
			log.Println("Successfully sent data pakcets to server", serverId)
		} else {
			log.Println("ERROR: Return status is false while sending data packets to server", serverId)
		}
	}
	return nil
}

func getUpdatesToPropagate() (map[int]shared.StabilizeDataPackets, error) {
	toPropagatePacketsAll := map[int]shared.StabilizeDataPackets {}
	for neighbor := range otherServers {
		toPropagatePackets := shared.StabilizeDataPackets {}
		if _, ok := propagatedToServer[neighbor]; !ok {
			propagatedToServer[neighbor] = map[int]int {}
		}
		neighborConnections := allConnections[neighbor]
		for k, v := range dataReceivedFrom {
			_, neighborConnected := neighborConnections[k]
			if propagatedToServer[neighbor][k] == 0 && !neighborConnected {
				toPropagatePackets[k] = v
			}
		}
		toPropagatePacketsAll[neighbor] = toPropagatePackets
	}
	return toPropagatePacketsAll, nil
}

func updateClockDuringStabilize(clock shared.Clock) {
	for k, v := range clock {
		if clockDuringStabilize[k] < v {
			clockDuringStabilize[k] = v
		}
	}
}

func updateClockAfterStabilize() {
	updateClockDuringStabilize(thisVecTs)
	for k, v := range clockDuringStabilize {
		thisVecTs[k] = v
	}
	incrementMyClock()
}

func cleanupStabilize() {
	// Resetting the flags and count at the end of stabilize
	numServersReceivedFrom = 0
	dataReceivedFrom = make(shared.StabilizeDataPackets)
	dbReceivedFrom = make(map[int]DB)
	isDataReceivedFrom = make(map[int]int)
	allConnections = make(map[int]map[int]string)
	receivedFromAllServers = false
	propagatedToServer = make(map[int]map[int]int)
}

func (t *ServerMaster) Stabilize(dummy int, status *bool) error {
	log.Println("Stabilize call received from master..")

	defer cleanupStabilize()

	// First send this server's inFlightData to the other servers
	err := sendMyDataToNeighbors()
	if err != nil {
		return err
	}

	// Loop to keep sending the neighbors' transitive updates to other neighbors
	intervalBetweenRetrySecs := 0.5
	for {
		updatesToPropagate, err := getUpdatesToPropagate()
		if err != nil {
			return err
		}
		// If no more updates to send to neighbors and if updates received from all servers
		if len(updatesToPropagate) == 0 && receivedFromAllServers {
			break
		}
		for neighbor, dataPackets := range updatesToPropagate {
			client, err := getServerRpcClient(neighbor)
			if err != nil {
				return err
			}

			log.Println("Sending transitive updates to server", neighbor, "with", len(dataPackets), "other server information..")

			var reply bool
			//TODO probably add a retry logic here because there are high chances for multiple servers to be propagating
			// data to the server at the same time in which case on will fail
			client.Call("ServerServer.SendDataPackets", dataPackets, &reply)
			if reply {
				log.Println("Successfully sent data pakcets to server", serverId)
			} else {
				log.Println("ERROR: Return status is false while sending data packets to server", serverId)
			}

			// Update the propagated information
			for k := range dataPackets {
				propagatedToServer[neighbor][k] = 1
			}
		}
		time.Sleep(time.Second * time.Duration(intervalBetweenRetrySecs))
	}

	log.Println("Received updates from all the connected servers.. Collating and resolving the conflicts.. ")

	// Now, wait until all other servers have sent their inFlightDbs to this server
/*	retryTimeSecs := 0.5
	retryLimit := 20
	retryAttempt := 0
	numOtherServers := len(otherServers)

	for {
		if numServersReceivedFrom >= numOtherServers {
			break
		}
		time.Sleep(time.Second * time.Duration(retryTimeSecs))
		retryAttempt++
		if retryAttempt >= retryLimit {
			return errors.New("did not get the updates from all the servers after " + strconv.Itoa(retryLimit) + " retries")
		}
	}
*/
	// Received updates from all other servers, now collate them and store to persistedDb and clear inFlightDb
	err = collateAndResolveConflicts()
	if err != nil {
		return err
	}
	updateClockAfterStabilize()

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

	log.Println("Listening to master on port", portToListen)
	l, e := net.Listen("tcp", "localhost:" + strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer l.Close()

	log.Println("Accepting connections from the listener in address", l.Addr())

	for {
		if shutDown {
			time.Sleep(time.Second) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to master thread..")
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
type ServerServer int
func (t *ServerServer) BootstrapData(dummy int, response *shared.BootstrapDataResponse) error {
	response.PersistedDb = map[string]shared.Value {}
	for k, v := range persistedDb {
		response.PersistedDb[k] = v
	}
	for k, v := range thisVecTs {
		response.VecTs[k] = v
	}
	return nil
}

func (t *ServerServer) SendDataPackets(request shared.StabilizeDataPackets, reply *bool) error {
	for serverId, dataPacket := range request {
		if isDataReceivedFrom[serverId] == 0 {
			isDataReceivedFrom[serverId] = 1
			dataReceivedFrom[serverId] = dataPacket
			dbReceivedFrom[serverId] = dataPacket.InFlightDB // Copy by reference. Should we copy by value?

			//for k, v := range dataPacket.InFlightDB {
			//	dataReceivedFrom[serverId][k] = v
			//}

			updateClockDuringStabilize(dataPacket.VecTs)
			connections := map[int]string {}
			for k, v := range dataPacket.Peers {
				connections[k] = v
				// k is the neighbor's neighbor
				if isDataReceivedFrom[k] != 1 {
					isDataReceivedFrom[k] = 0
				}
			}
			allConnections[serverId] = connections
			numServersReceivedFrom++
		}
	}
	if numServersReceivedFrom >= len(isDataReceivedFrom) {
		receivedFromAllServers = true
	}

	*reply = true
	return nil
}

func listenToServers() error {
	defer waitGroup.Done()
	portToListen := thisBasePort + 1

	serverServer := new(ServerServer)
	rpcServer := rpc.NewServer()
	log.Println("Registering ServerServer..")
	rpcServer.RegisterName("ServerServer", serverServer)

	log.Println("Listening to servers on port", portToListen)
	l, e := net.Listen("tcp", util.LOCALHOST_PREFIX + strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer l.Close()

	log.Println("Accepting connections from the listener in address", l.Addr())

	for {
		if shutDown {
			time.Sleep(time.Second) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to servers thread..")
			break
		}
		log.Println("Listening to connection from the servers..")
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
Implementation of different RPC methods exported by server to clients
*/
type ServerClient int

func (t *ServerClient) ServerPut(putArgs shared.ClientToServerPutArgs, reply *shared.Clock) error {
	// TODO: Handle Multiple Clients
	// Resolve putArgs parameter
	key := putArgs.Key
	value := putArgs.Value
	clientId := putArgs.ClientId
	clientClock := putArgs.ClientClock

	// Increment servers logical clock on receiving a put request from the client
	incrementMyClock()
	// Update the client timeStamp
	if thisVecTs[clientId] < clientClock {
		thisVecTs[clientId] = clientClock
	}

	// Update/Write to the inFlightDb
	newValue := shared.Value{value, thisVecTs, thisServerId, clientId}
	prevValue, exists := inFlightDb[key]
	if exists == false {
		// Create a new entry into the inFlightDb
		inFlightDb[key] = newValue
		*reply = newValue.Ts
	} else {
		// Compare the timeStamps of the values and either update or ignore
		ordering := util.TotalOrderOfEvents(prevValue.Ts, prevValue.ServerId, newValue.Ts, newValue.ServerId)
		if ordering == util.HAPPENED_BEFORE {
			inFlightDb[key] = newValue
			*reply = newValue.Ts
		} else {
			*reply = prevValue.Ts
		}
	}
	// Set the timestamp of this transaction to the return value
	return nil
}

func (t *ServerClient) ServerGet(key string, reply *shared.Value) error {
	// TODO: Handle Multiple Clients
	// Increment the clock on receiving a get request from client
	incrementMyClock()

	// Check for the key in inFlightDb first.
	value, ok := inFlightDb[key]
	if ok {
		*reply = value
		return nil
	}
	// Check for the key in persistedDb
	value, ok = persistedDb[key]
	if ok {
		*reply = value
		return nil
	}
	// key is not found in both inFlightDb and persistedDb
	return errors.New("ERR_NO_KEY")
}

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

func incrementMyClock() {
	thisVecTs[thisServerId]++
}

func bootstrapFromServer(serverId int) error {
	client, err := getServerRpcClient(serverId)
	if err != nil {
		return err
	}

	log.Println("Getting bootstrap data from server", serverId)
	var reply shared.BootstrapDataResponse
	client.Call("ServerServer.BootstrapData", 0, &reply)
	for k, v := range reply.PersistedDb {
		persistedDb[k] = v
	}
	for k, v := range reply.VecTs {
		if v > thisVecTs[k] {
			thisVecTs[k] = v
		}
	}
	incrementMyClock()

	log.Println("Bootstrap complete. Fetched", len(persistedDb), "key-value pairs from server", serverId)
	return nil
}

func bootstrapData() error {
	numOtherServers := len(otherServers)
	if numOtherServers == 0 {
		log.Println("No other servers found to bootstrap data..")
		return nil
	}
	r := rand.Int() % numOtherServers
	serverToBootstrapFrom := -1
	for k := range otherServers {
		if r == 0 {
			serverToBootstrapFrom = k
			break
		}
		r--
	}
	err := bootstrapFromServer(serverToBootstrapFrom)
	return err
}

func initializeClock() {
	thisVecTs[thisServerId] = 1 // or, 0
}

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

	initializeClock()

	err = bootstrapData()
	if err != nil {
		log.Fatal(err)
	}

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
