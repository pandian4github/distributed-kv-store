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

var thisServerVecTs = shared.Clock{}

// Maps serverId to host:basePort of that server
var otherServers = map[int]string{}

// Maps serverId to its logical clock during its latest stabilize that this server knows about
var stabilizeCheckpoint = map[int]int{}

// Maintains a map of open connections to the other servers
//var serverRpcClientMap = map[int]*rpc.Client {}

// To wait for all threads to complete before exiting
var waitGroup sync.WaitGroup

// Flag which is set during a killServer
var serverShutDown = false

// Listener object to listen to other servers
var serverListener net.Listener
var clientListener net.Listener

var mutex = &sync.Mutex{}

type ServerPacketPair struct {
	ServerId int
	DataPacketId int // data packet belonging to the server
}

// Flag to check if data is received from all other servers during a stabilize call
var dataReceivedFrom = map[int]shared.StabilizeDataPacket{}
var dbReceivedFrom = map[int]DB{}
var isDataReceivedFrom = map[int]int{}
var numServersReceivedFrom = 0
var allConnections = map[int]map[int]string{}
var propagatedToServer = map[int]map[int]int{} // {x: y} to denote if y's data is already propagated to x
var clockDuringStabilize = shared.Clock{}
var serverHasDataPacketMap = map[ServerPacketPair]int{}

// Core data structures which hold the actual data of the key-value store
type DB map[string]shared.Value

var persistedDb = DB{} // the DB after the last stabilize call
var inFlightDb = DB{}  // the DB which is not yet stabilized with other server nodes

func getServerRpcClient(serverId int) (*rpc.Client, error) {
	// Commenting because using open connection led to some issues
	//if client, ok := serverRpcClientMap[serverId]; ok {
	//	return client, nil
	//}
	if hostBasePortPair, ok := otherServers[serverId]; ok {
		basePortStr := strings.Split(hostBasePortPair, ":")[1]
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
		//serverRpcClientMap[serverId] = client
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
		// TODO Should also kill the other listening threads, otherwise the ports are blocked for sometime (make the listener objects global?)
		serverShutDown = true
		log.Println("Marked serverShutDown flag..")
	} else { // Remove the server details from the map
		log.Println("Removing server", serverId, "from the cluster..")
		delete(otherServers, serverId)
	}
	*status = true
	return nil
}

func (t *ServerMaster) KillServer(dummy int, status *bool) error {
	serverShutDown = true
	log.Println("Marked serverShutDown flag..")
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

func updateInflightWithPersistedDB(otherPersistedDB map[string]shared.Value) error {
	numKeysUpdated := 0
	for k, v := range otherPersistedDB {
		existingValue, present := Get(k)
		if !present || util.TotalOrderOfEvents(existingValue.Ts, existingValue.ServerId, v.Ts, v.ServerId) == util.HAPPENED_BEFORE{
			// If there is not existing value of if existing value happened before the value from the other server, just add it to the inFlightDB
			inFlightDb[k] = v
			numKeysUpdated++
		}
		// If existing value happened after the value from the other server, ignore it since it will anyways be discarded during stabilize
	}
	log.Println(numKeysUpdated, "keys have been updated from the persistedDB of the other server.")
	return nil
}

func syncPersistedDBsIfNotInSync(serverId int) error {
	client, err := getServerRpcClient(serverId)
	if err != nil {
		return err
	}

	log.Println("Syncing PersistedDB with server", serverId)
	var request = shared.SyncStabilizeCheckpointRequest{ServerId: thisServerId, StabilizeCheckpoint: stabilizeCheckpoint}
	var response shared.SyncStabilizeCheckpointResponse

	client.Call("ServerServer.SyncStabilizeCheckpoint", request, &response)
	if response.NotInSync {
		log.Println("Servers are not in sync. Updating inFlightDB with the other server's PersistedDB..")
		updateInflightWithPersistedDB(response.PersistedDB)
		util.UpdateMyClock(&thisServerVecTs, response.VecTs)
	} else {
		log.Println("Servers are already in sync.")
	}
	client.Close()

	return nil
}

func (t *ServerMaster) CreateConnection(newServer *shared.NewServerArgs, status *bool) error {
	serverId := newServer.ServerId
	hostPortPair := newServer.HostPortPair
	log.Println("Creating connection between this server", thisServerId, "and server", serverId)
	otherServers[serverId] = hostPortPair

	syncPersistedDBsIfNotInSync(serverId)

	*status = true
	return nil
}

func (t *ServerMaster) PrintStore(dummy int, response *map[string]string) error {
	for k, v := range persistedDb {
		(*response)[k] = v.Val
	}
	for k, v := range inFlightDb {
		(*response)[k] = v.Val
	}
	return nil
}

func collateAndResolveConflicts() error {
	// Updating persistedDb after resolving conflicts from the other server's updates
	dbReceivedFrom[thisServerId] = inFlightDb
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
	// After all the updates in flight are persisted clear the inFlightDB
	inFlightDb = make(DB)
	return nil
}

func sendMyDataToNeighbors() error {
	mutex.Lock()
	stabilizeCheckpoint[thisServerId] = thisServerVecTs[thisServerId]
	for serverId := range otherServers {
		if isDataReceivedFrom[serverId] != 1 {
			isDataReceivedFrom[serverId] = 0
		}
	}
	mutex.Unlock()

	for serverId := range otherServers {
		client, err := getServerRpcClient(serverId)
		if err != nil {
			return err
		}

		log.Println("Sending inFlightData to server", serverId)
		var thisServerDataPacket = shared.StabilizeDataPacket{InFlightDB: inFlightDb, VecTs: thisServerVecTs, Peers: otherServers}
		var args = shared.SendDataPacketsRequest{ServerId: thisServerId, DataPackets: shared.StabilizeDataPackets{thisServerId: thisServerDataPacket}}

		var reply bool
		client.Call("ServerServer.SendDataPackets", args, &reply)
		if reply {
			log.Println("Successfully sent data pakcets to server", serverId)
		} else {
			log.Println("ERROR: Return status is false while sending data packets to server", serverId)
		}
		client.Close()
	}
	return nil
}

func getUpdatesToPropagate() (map[int]shared.StabilizeDataPackets, error) {
	toPropagatePacketsAll := map[int]shared.StabilizeDataPackets{}
	for neighbor := range otherServers {
		toPropagatePackets := shared.StabilizeDataPackets{}
		if _, ok := propagatedToServer[neighbor]; !ok {
			propagatedToServer[neighbor] = map[int]int{}
		}

		mutex.Lock()
		neighborConnections := allConnections[neighbor]
		for dataPacketId, dataPacket := range dataReceivedFrom {
			_, neighborConnected := neighborConnections[dataPacketId]
			if propagatedToServer[neighbor][dataPacketId] == 0 && !neighborConnected && neighbor != dataPacketId &&
				serverHasDataPacketMap[ServerPacketPair{ServerId: neighbor, DataPacketId: dataPacketId}] == 0 {
				toPropagatePackets[dataPacketId] = dataPacket
			}
		}
		mutex.Unlock()

		if len(toPropagatePackets) > 0 {
			toPropagatePacketsAll[neighbor] = toPropagatePackets
		}
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
	updateClockDuringStabilize(thisServerVecTs)
	for k, v := range clockDuringStabilize {
		thisServerVecTs[k] = v
	}
	incrementMyServerClock()
}

func cleanupStabilize() {
	log.Println("Cleaning metadata for stabilize..")
	// Resetting the flags and count at the end of stabilize
	numServersReceivedFrom = 1 // this server's information
	dataReceivedFrom = make(shared.StabilizeDataPackets)
	dbReceivedFrom = make(map[int]DB)
	isDataReceivedFrom = make(map[int]int)
	isDataReceivedFrom[thisServerId] = 1
	allConnections = make(map[int]map[int]string)
	propagatedToServer = make(map[int]map[int]int)
	serverHasDataPacketMap = make(map[ServerPacketPair]int)
}

func (t *ServerMaster) Stabilize(dummy int, status *bool) error {
	log.Println("Stabilize call received from master..")

	defer cleanupStabilize()

	incrementMyServerClock()

	// First send this server's inFlightData to the other servers
	err := sendMyDataToNeighbors()
	log.Println("Sent data to my neighbors.. isDataReceivedFrom: ", isDataReceivedFrom)
	if err != nil {
		return err
	}

	// Loop to keep sending the neighbors' transitive updates to other neighbors
	var intervalBetweenRetrySecs float32 = 0.005
	for {
		updatesToPropagate, err := getUpdatesToPropagate()
		if len(updatesToPropagate) > 0 {
			log.Println("Transitive updates to propagate: ", updatesToPropagate)
		}
		if err != nil {
			return err
		}
		// If no more updates to send to neighbors and if updates received from all servers
		if len(updatesToPropagate) == 0 && numServersReceivedFrom >= len(isDataReceivedFrom) {
			// Explicitly check again if there is any update to propagate due to concurrency issues
			updatesToPropagate, _ = getUpdatesToPropagate()
			if len(updatesToPropagate) == 0 {
				break
			}
		}
		for neighbor, dataPackets := range updatesToPropagate {
			client, err := getServerRpcClient(neighbor)
			if err != nil {
				return err
			}

			log.Println("Sending transitive updates to server", neighbor, "with", len(dataPackets), "other server information..")

			var reply bool
			args := shared.SendDataPacketsRequest{ServerId: thisServerId, DataPackets: dataPackets}
			//TODO probably add a retry logic here because there are high chances for multiple servers to be propagating
			// data to the server at the same time in which case on will fail
			client.Call("ServerServer.SendDataPackets", args, &reply)
			if reply {
				log.Println("Successfully sent data packets to server", neighbor)
			} else {
				log.Println("ERROR: Return status is false while sending data packets to server", neighbor)
			}

			// Update the propagated information
			for k := range dataPackets {
				propagatedToServer[neighbor][k] = 1
			}
			client.Close()
		}
		util.Sleep(intervalBetweenRetrySecs)
	}

	log.Println("Received updates from all the connected servers.. Collating and resolving the conflicts.. ")

	// Received updates from all other servers, now collate them and store to persistedDb and clear inFlightDb
	err = collateAndResolveConflicts()
	if err != nil {
		return err
	}
	updateClockAfterStabilize()

	//cleanupStabilize()
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
	l, e := net.Listen("tcp", "localhost:"+strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer l.Close()

	log.Println("Accepting connections from the listener in address", l.Addr())

	for {
		if serverShutDown {
			util.Sleep(1.0) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to master thread..")
			break
		}
		log.Println("Listening to connection from the master..")
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
			return err
		}
		rpcServer.ServeConn(conn) // synchronous call required?
	}

	log.Println("Closing serverListener..")
	serverListener.Close()
	log.Println("Closing clientListener..")
	clientListener.Close()

	return nil
}

/*
Implementation of different RPC methods exported by server to other servers
*/
type ServerServer int

func (t *ServerServer) BootstrapData(serverId int, response *shared.BootstrapDataResponse) error {
	log.Println("Got bootstrap request from server", serverId)
	response.PersistedDb = map[string]shared.Value{}
	response.VecTs = shared.Clock{}
	for k, v := range persistedDb {
		response.PersistedDb[k] = v
	}
	for k, v := range thisServerVecTs {
		response.VecTs[k] = v
	}
	log.Println("Bootstrap request complete, data copied to response object.")
	return nil
}

func (t *ServerServer) SendDataPackets(request shared.SendDataPacketsRequest, reply *bool) error {
	mutex.Lock()

	dataPackets := request.DataPackets
	for serverId, dataPacket := range dataPackets {
		log.Println("Received data packet of server", serverId, "from server", request.ServerId, "dataPacket: ", dataPacket)
		stabilizeCheckpoint[serverId] = dataPacket.VecTs[serverId]
		if thisServerVecTs[serverId] >= dataPacket.VecTs[serverId] {
			log.Println("Ignoring data packet of server", serverId, "with ts ", dataPacket.VecTs, "my ts", thisServerVecTs)
			continue
		}
		serverHasDataPacketMap[ServerPacketPair{ServerId: request.ServerId, DataPacketId: serverId}] = 1
		if isDataReceivedFrom[serverId] == 0 {
			isDataReceivedFrom[serverId] = 1

			dataReceivedFrom[serverId] = dataPacket
			dbReceivedFrom[serverId] = dataPacket.InFlightDB // Copy by reference. Should we copy by value?

			log.Println("Updating the vector clock according to vector clock of server", serverId)
			updateClockDuringStabilize(dataPacket.VecTs)
			connections := map[int]string{}
			for k, v := range dataPacket.Peers {
				connections[k] = v
				// k is the neighbor's neighbor
				if isDataReceivedFrom[k] != 1 {
					isDataReceivedFrom[k] = 0
				}
			}
			allConnections[serverId] = connections

			numServersReceivedFrom++
			log.Println("isDataReceivedFrom:", isDataReceivedFrom, "numServersReceivedFrom:", numServersReceivedFrom)
		}
	}
	if numServersReceivedFrom >= len(isDataReceivedFrom) {
		log.Println("Received data from all connected servers: ", numServersReceivedFrom, "isDataReceivedFrom: ", isDataReceivedFrom)
	}
	mutex.Unlock()

	*reply = true
	return nil
}

func (t *ServerServer) SyncStabilizeCheckpoint(request shared.SyncStabilizeCheckpointRequest, reply *shared.SyncStabilizeCheckpointResponse) error {
	mutex.Lock()
	otherCurrentCheckpoint := request.StabilizeCheckpoint[thisServerId]

	(*reply).ServerId = thisServerId
	(*reply).VecTs = thisServerVecTs
	if otherCurrentCheckpoint >= stabilizeCheckpoint[thisServerId] { // the server is already in sync
		(*reply).NotInSync = false
		(*reply).PersistedDB = make(map[string]shared.Value)
	} else {
		(*reply).NotInSync = true
		(*reply).PersistedDB = persistedDb
	}
	mutex.Unlock()
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
	var e error
	serverListener, e = net.Listen("tcp", util.LOCALHOST_PREFIX+strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer serverListener.Close()

	log.Println("Accepting connections from the listener in address", serverListener.Addr())

	for {
		if serverShutDown {
			util.Sleep(1.0) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to servers thread..")
			break
		}
		log.Println("Listening to connection from the servers..")
		conn, err := serverListener.Accept()
		if err != nil {
			log.Fatal(err)
			return err
		}
		rpcServer.ServeConn(conn) // synchronous call required?
	}

	return nil
}

/*
Implementation of different RPC methods exported by server to clients
*/
type ServerClient int

func (t *ServerClient) ServerPut(putArgs shared.ClientToServerPutArgs, reply *[2]shared.Clock) error {
	key := putArgs.Key
	value := putArgs.Value
	clientId := putArgs.ClientId
	clientClock := putArgs.ClientClock

	incrementMyServerClock()
	util.UpdateMyClock(&thisServerVecTs, clientClock)

	// Update/Write to the inFlightDb
	var valTs = make(map[int]int)
	util.CopyVecTs(thisServerVecTs, &valTs)
	newValue := shared.Value{Val: value, Ts: valTs, ServerId: thisServerId, ClientId: clientId}
	prevValue, exists := inFlightDb[key]

	if exists == false {
		inFlightDb[key] = newValue
		reply[0] = newValue.Ts
	} else {
		// Compare the timeStamps of the values and either update or ignore
		ordering := util.TotalOrderOfEvents(prevValue.Ts, prevValue.ServerId, newValue.Ts, newValue.ServerId)
		if ordering == util.HAPPENED_BEFORE {
			inFlightDb[key] = newValue
			reply[0] = newValue.Ts
		} else {
			reply[0] = prevValue.Ts
		}
	}
	reply[1] = thisServerVecTs
	return nil
}

func Get(key string) (shared.Value, bool) {
	value, ok := inFlightDb[key]
	if ok {
		return value, true
	}

	value, ok = persistedDb[key]
	if ok {
		return value, true
	}

	return shared.Value{}, false
}

func (t *ServerClient) ServerGet(getArgs shared.ClientToServerGetArgs, reply *shared.ServerToClientGetReply) error {
	incrementMyServerClock()
	util.UpdateMyClock(&thisServerVecTs, getArgs.ClientVecTs)

	value, found := Get(getArgs.Key)
	if !found {
		value.Val = "ERR_KEY"
		value.ServerId = -1
		value.ClientId = -1
	}
	reply.Value = value
	reply.ServerVecTs = thisServerVecTs

	return nil
}

func listenToClients() error {
	defer waitGroup.Done()
	portToListen := thisBasePort + 2

	serverClient := new(ServerClient)
	rpcServer := rpc.NewServer()
	log.Println("Registering ServerClient..")
	rpcServer.RegisterName("ServerClient", serverClient)

	log.Println("Listening to clients on port", portToListen)
	var e error
	clientListener, e = net.Listen("tcp", util.LOCALHOST_PREFIX+strconv.Itoa(portToListen))
	if e != nil {
		log.Fatal(e)
	}
	defer clientListener.Close()

	log.Println("Accepting connections from the client listener in address", clientListener.Addr())

	for {
		if serverShutDown {
			util.Sleep(1.0) // so that the RPC call returns before the process is shut down
			log.Println("Shutting down listen to clients thread..")
			break
		}
		log.Println("Listening to connection from the clients..")
		conn, err := clientListener.Accept()
		//stopStabilize = false
		if err != nil {
			log.Fatal(err)
			return err
		}
		rpcServer.ServeConn(conn) // synchronous call required?
	}

	return nil
}

func incrementMyServerClock() {
	log.Print("Incrementing server clock")
	thisServerVecTs[thisServerId]++
}

func bootstrapFromServer(serverId int) error {
	client, err := getServerRpcClient(serverId)
	defer client.Close()
	if err != nil {
		return err
	}

	log.Println("Getting bootstrap data from server", serverId)
	var reply shared.BootstrapDataResponse
	client.Call("ServerServer.BootstrapData", thisServerId, &reply)
	for k, v := range reply.PersistedDb {
		persistedDb[k] = v
	}
	for k, v := range reply.VecTs {
		if v > thisServerVecTs[k] {
			thisServerVecTs[k] = v
		}
	}
	incrementMyServerClock()

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
	thisServerVecTs[thisServerId] = 1 // or, 0
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

	cleanupStabilize()

	// since we are starting three threads
	waitGroup.Add(3)

	go listenToMaster()
	go listenToServers()
	go listenToClients()

	waitGroup.Wait()
}
