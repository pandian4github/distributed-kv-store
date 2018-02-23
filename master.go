package main

import (
	"fmt"
	"os"
	"log"
	"bufio"
	"strings"
	"strconv"
	"errors"
	"./util"
	"os/exec"
	"net/rpc"
	"./shared"
	"sync"
)

var functionMap = map[string]func(args []string) error {
	"joinServer": joinServer,
	"killServer": killServer,
	"joinClient": joinClient,
	"breakConnection": breakConnection,
	"createConnection": createConnection,
	"stabilize": stabilize,
	"printStore": printStore,
	"put": put,
	"get": get,
}

var numArgsMap = map[string]int {
	"joinServer": 1,
	"killServer": 1,
	"joinClient": 2,
	"breakConnection": 2,
	"createConnection": 2,
	"stabilize": 0,
	"printStore": 1,
	"put": 3,
	"get": 2,
}

// Starting from 5001, the servers and clients are made to listen on different ports
var portsUsed = 5001

// Maps the server/client ids to the ports in which they would be listening
var portMapping = map[int]int {}

// Maintains if a server/client is active or not
//var isNodeAlive = map[int]bool {}

// Whether the node is a server or client (0 for server and 1 for client)
var nodeType = map[int]int {}

// Maintains a map of open connections to different servers and clients
var masterRpcClientMap = map[int]*rpc.Client {}

// To distinguish between a server and a client node
const NODE_SERVER = 0
const NODE_CLIENT = 1

func getMasterRpcClient(nodeId int) (*rpc.Client, error) {
	if client, ok := masterRpcClientMap[nodeId]; ok {
		return client, nil
	}

	hostPortPair := util.LOCALHOST_PREFIX + strconv.Itoa(portMapping[nodeId])
	conn, err := util.DialWithRetry(hostPortPair)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClient(conn)
	masterRpcClientMap[nodeId] = client
	return client, nil
}

func closeAllClients() error {
	for nodeId, client := range masterRpcClientMap {
		log.Println("Closing RPC client to nodeId", nodeId)
		client.Close()
	}
	return nil
}

func joinServer(args []string) error {
	// TODO when a new server is added, it should fetch the persistedDb so far from the other nodes. It should be implemented.
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	// If the node with this id is already alive in the system
	if _, ok := portMapping[serverId]; ok {
		return errors.New("Node with id " + strconv.Itoa(serverId) + " is already alive!")
	}

	basePort := portsUsed
	portsUsed += 3 // since server listens on three different threads
	portMapping[serverId] = basePort
	nodeType[serverId] = NODE_SERVER
	hostBasePortPair := util.LOCALHOST_PREFIX + strconv.Itoa(basePort)

	// Fetch the other server details from global portMapping (which includes both server and client)
	// map of serverId to host:port
	otherServers := map[int]string {}

	for k, v := range portMapping {
		if nodeType, ok := nodeType[k]; ok {
			if nodeType == NODE_SERVER && k != serverId {
				otherServers[k] = util.LOCALHOST_PREFIX + strconv.Itoa(v)
			}
		}
	}

	otherServersStr, err := util.EncodeMapIntStringToStringCustom(otherServers)
	if err != nil {
		log.Fatal(err)
	}

	logFileName := "logs/server" + strconv.Itoa(serverId) + ".log"
	log.Println("Starting a go process for server", serverId, "with stdout", logFileName)

	cmd := exec.Command("go", "run", "server.go", strconv.Itoa(serverId),
		strconv.Itoa(basePort), otherServersStr)

	// TODO: need to close this file somewhere
	out, err := os.Create(logFileName)
	if err != nil {
		return nil
	}
	cmd.Stdout = out
	cmd.Stderr = out

	cmd.Start()

	//isNodeAlive[serverId] = true

	/*
	TODO: Notify other servers about this new server added using RPC calls.
	 */
	for k := range otherServers {
		client, err := getMasterRpcClient(k)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Broadcasting the new server information to the server", k)
		var reply bool
		args := &shared.NewServerArgs{ServerId:serverId, HostPortPair:hostBasePortPair}
		client.Call("ServerMaster.AddNewServer", args, &reply)
		if reply {
			log.Println("Successfully broadcasted.")
		} else {
			log.Fatal("Reply status is false. Broadcast failed.")
		}
	}

	return nil
}

func killServer(args []string) error {
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}

	for k := range portMapping {
		if nodeType, ok := nodeType[k]; ok && nodeType == NODE_SERVER {
			client, err := getMasterRpcClient(k)
			if err != nil {
				log.Fatal(err)
			}

			log.Println("Broadcasting the server information to remove to the server", k)
			var reply bool
			args := &shared.RemoveServerArgs{ServerId:serverId}
			client.Call("ServerMaster.RemoveServer", args, &reply)
			if reply {
				log.Println("Successfully broadcasted.")
			} else {
				log.Fatal("Reply status is false. Broadcast failed.")
			}
		}
	}

	// If server is successfully killed, remove it from active list and portMapping
	delete(portMapping, serverId)
	//isNodeAlive[serverId] = false

	return nil
}

func joinClient(args []string) error {
	clientId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	serverId, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}

	// If the node with this id is already alive in the system
	if _, ok := portMapping[clientId]; ok {
		return errors.New("Node with id " + strconv.Itoa(serverId) + " is already alive!")
	}

	basePort := portsUsed
	portsUsed++ // client need to listen on only one port (for master to communicate)
	portMapping[clientId] = basePort
	nodeType[clientId] = NODE_CLIENT

	logFileName := "logs/client" + strconv.Itoa(clientId) + ".log"
	log.Println("Starting a go process for client", clientId, "with stdout", logFileName)

	cmd := exec.Command("go", "run", "client.go", strconv.Itoa(clientId),
		strconv.Itoa(serverId), strconv.Itoa(portMapping[serverId]), strconv.Itoa(basePort))

	// TODO: need to close this file somewhere
	out, err := os.Create(logFileName)
	if err != nil {
		return nil
	}
	cmd.Stdout = out
	cmd.Stderr = out

	cmd.Start()

	return nil
}

/*
Remove server information from client
*/
func removeConnectionBetweenClientServer(clientId int, serverId int) error {
	client, err := getMasterRpcClient(clientId)
	if err != nil {
		return err
	}

	log.Println("Removing server information of", serverId, "from client", clientId)
	var reply bool
	args := &shared.RemoveServerArgs{ServerId:serverId}
	client.Call("ClientMaster.BreakConnection", args, &reply)
	if reply {
		log.Println("Successfully removed connection of", serverId, "from", clientId)
	} else {
		log.Fatal("Reply status is false. Break connection failed.")
	}

	return nil
}


/*
Remove s2 information from s1 server
*/
func removeConnectionBetweenServers(s1 int, s2 int) error {
	client, err := getMasterRpcClient(s1)
	if err != nil {
		return err
	}

	log.Println("Removing server information of", s2, "from server", s1)
	var reply bool
	args := &shared.RemoveServerArgs{ServerId:s2}
	client.Call("ServerMaster.BreakConnection", args, &reply)
	if reply {
		log.Println("Successfully removed connection of", s2, "from", s1)
	} else {
		log.Fatal("Reply status is false. Break connection failed.")
	}

	return nil
}

/*
Add s2 information to s1 server
*/
func addConnectionBetweenClientServer(clientId int, serverId int) error {
	client, err := getMasterRpcClient(clientId)
	if err != nil {
		return err
	}

	log.Println("Adding server information of", serverId, "to client", clientId)
	var reply bool
	args := &shared.ClientServerConnectionArgs{ServerId:serverId, ServerBasePort:portMapping[serverId]}
	client.Call("ClientMaster.CreateConnection", args, &reply)
	if reply {
		log.Println("Successfully added connection of", serverId, "to", clientId)
	} else {
		log.Fatal("Reply status is false. Create connection failed.")
	}

	return nil
}


/*
Add s2 information to s1 server
*/
func addConnectionBetweenServers(s1 int, s2 int) error {
	client, err := getMasterRpcClient(s1)
	if err != nil {
		return err
	}

	log.Println("Adding server information of", s2, "to server", s1)
	var reply bool
	args := &shared.NewServerArgs{ServerId:s2, HostPortPair:util.LOCALHOST_PREFIX +
		strconv.Itoa(portMapping[s2])}
	client.Call("ServerMaster.CreateConnection", args, &reply)
	if reply {
		log.Println("Successfully added connection of", s2, "to", s1)
	} else {
		log.Fatal("Reply status is false. Create connection failed.")
	}

	return nil
}

func breakConnection(args []string) error {
	nodeId1, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	nodeId2, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}
	//log.Println(portMapping)

	if _, ok1 := portMapping[nodeId1]; ok1 {
		if _, ok2 := portMapping[nodeId2]; ok2 {
			if nodeType[nodeId1] == NODE_CLIENT && nodeType[nodeId2] == NODE_CLIENT {
				return errors.New("cannot break connection between two clients")
			}

			if nodeType[nodeId1] == nodeType[nodeId2] { // connection between two servers
				err := removeConnectionBetweenServers(nodeId1, nodeId2)
				if err != nil {
					return err
				}
				err = removeConnectionBetweenServers(nodeId2, nodeId1)
				if err != nil {
					return err
				}
			} else { // connection between a server and a client
				if nodeType[nodeId1] == NODE_CLIENT {
					removeConnectionBetweenClientServer(nodeId1, nodeId2)
				} else {
					removeConnectionBetweenClientServer(nodeId2, nodeId1)
				}
			}

			// If successful
			return nil
		}
	}
	return errors.New("either the nodes are not alive in the system or are not started yet")
}

func createConnection(args []string) error {
	nodeId1, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	nodeId2, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}

	if _, ok1 := portMapping[nodeId1]; ok1 {
		if _, ok2 := portMapping[nodeId2]; ok2 {
			if nodeType[nodeId1] == NODE_CLIENT && nodeType[nodeId2] == NODE_CLIENT {
				return errors.New("cannot create connection between two clients")
			}

			if nodeType[nodeId1] == nodeType[nodeId2] { // connection between two servers
				addConnectionBetweenServers(nodeId1, nodeId2)
				addConnectionBetweenServers(nodeId2, nodeId1)
			} else { // connection between a server and a client
				if nodeType[nodeId1] == NODE_CLIENT {
					addConnectionBetweenClientServer(nodeId1, nodeId2)
				} else {
					addConnectionBetweenClientServer(nodeId2, nodeId1)
				}
			}

			// If successful
			return nil
		}
	}
	return errors.New("either the nodes are not alive in the system or are not started yet")
}

func stabilizeAsync(serverId int, waitTillStabilize *sync.WaitGroup) {
	defer waitTillStabilize.Done()

	client, err := getMasterRpcClient(serverId)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Sending stabilize call to server", serverId)
	var reply bool
	client.Call("ServerMaster.Stabilize", 0, &reply)

	if reply {
		log.Println("Successfully stabilized server", serverId)
	} else {
		log.Fatal("reply status is false. Stabilize command failed")
	}
}

func stabilize(args []string) error {
	var waitTillStabilize sync.WaitGroup // thread-safe?
	for k := range portMapping {
		if nodeType, ok := nodeType[k]; ok && nodeType == NODE_SERVER {
			waitTillStabilize.Add(1)
			go stabilizeAsync(k, &waitTillStabilize)
		}
	}
	// Wait till the stabilize completes on all servers
	waitTillStabilize.Wait()
	return nil
}

func printStore(args []string) error {
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}

	// If the node with this id is not alive in the system
	if _, ok := portMapping[serverId]; !ok {
		return errors.New("Node with id " + strconv.Itoa(serverId) +
			" does not exist or is not alive!")
	}

	client, err := getMasterRpcClient(serverId)
	if err != nil {
		return err
	}

	log.Println("Printing the DB contents of server", serverId)
	var reply bool
	client.Call("ServerMaster.PrintStore", 0, &reply)
	if reply {
		log.Println("Successfully printed DB contents of server", serverId)
	} else {
		log.Fatal("Reply status is false. PrintStore command failed.")
	}

	return nil
}

func put(args []string) error {
	clientId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	key := args[2]
	value := args[3]

	// If the node with this id is not alive in the system
	if _, ok := portMapping[clientId]; !ok {
		return errors.New("Node with id " + strconv.Itoa(clientId) + " does not exist or is not alive!")
	}

	client, err := getMasterRpcClient(clientId)
	if err != nil {
		log.Fatal("Failed: rpc.Dail from Master to Client", err)
		return err
	}
	var reply bool
	putArgs := shared.MasterToClientPutArgs{key, value}
	err = client.Call("ClientMaster.ClientPut", putArgs, &reply)
	if err != nil {
		log.Fatal(err)
	}

	if reply != true {
		log.Fatal("ClientPut was not successful!")
	}

	// If successful
	return nil
}

func get(args []string) error {
	clientId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	key := args[2]

	// If the node with this id is not alive in the system
	if _, ok := portMapping[clientId]; !ok {
		return errors.New("Node with id " + strconv.Itoa(clientId) +
			" does not exist or is not alive!")
	}
	client, err := getMasterRpcClient(clientId)
	if err != nil {
		log.Fatal("Failed: rpc.Dail from Master to Client", err)
		return err
	}
	var reply bool
	err = client.Call("ClientMaster.ClientGet", key, &reply)
	if err != nil {
		log.Fatal(err)
	}

	if reply != true {
		log.Fatal("ClientGet was not successful")
	}

	// If successful
	return nil
}

func main() {
	args := os.Args

	if len(args) != 2 {
		fmt.Println("Program should contain one argument (path to the file containing the commands)")
		os.Exit(1)
	}
	commandsFile := args[1]
	log.Println("Commands file:", commandsFile)
	file, err := os.Open(commandsFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	defer closeAllClients()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		numParts := len(parts)
		if numParts == 0 {
			continue
		}
		if function, ok := functionMap[parts[0]]; ok {
			if numParts - 1 != numArgsMap[parts[0]] {
				log.Fatal("Command ", parts[0], " should have exactly ", numArgsMap[parts[0]], " argument(s).")
			}
			err := function(parts)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Println("Error: unknown command to the key-value store", parts[0])
		}
		log.Println("Execution over for command:", line)
		log.Println("------------------------------------------------------------")
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
