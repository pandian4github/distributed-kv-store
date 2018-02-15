package main

import (
	"fmt"
	"os"
	"log"
	"bufio"
	"strings"
	"strconv"
	"errors"
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
var isNodeAlive = map[int]bool {}

// Whether the node is a server or client (0 for server and 1 for client)
var nodeType = map[int]int {}

// To distinguish between a server and a client node
const NODE_SERVER = 0
const NODE_CLIENT = 1

func joinServer(args []string) error {
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	// If the node with this id is already alive in the system
	if nodeAlive, ok := isNodeAlive[serverId]; ok && nodeAlive {
		return errors.New("Node with id " + strconv.Itoa(serverId) + " is already alive!")
	}

	basePort := portsUsed
	portsUsed += 3 // since server listens on three different threads
	portMapping[serverId] = basePort
	nodeType[serverId] = NODE_SERVER

	/*
	Here goes the code to start a new server (arguments are base_port to which it should listen,
	serverId and the other server-port details). Also, notify the other servers about this
	new server added to the cluster.
	*/

	isNodeAlive[serverId] = true

	return nil

}

func killServer(args []string) error {
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}

	/*
	Here goes the code to kill a server and notify all other servers that the specified server
	is killed.
	*/

	// If server is successfully killed, remove it from active list
	isNodeAlive[serverId] = false

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
	if nodeAlive, ok := isNodeAlive[clientId]; ok && nodeAlive {
		return errors.New("Node with id " + strconv.Itoa(serverId) + " is already alive!")
	}

	basePort := portsUsed
	portsUsed++ // client need to listen on only one port (for master to communicate)
	portMapping[clientId] = basePort
	nodeType[clientId] = NODE_CLIENT

	/*
	Here goes the code to start a client with the basePort and serverId as the parameters.
	*/

	isNodeAlive[clientId] = true

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

	if node1Alive, ok1 := isNodeAlive[nodeId1]; ok1 {
		if node2Alive, ok2 := isNodeAlive[nodeId2]; ok2 {
			// Both nodes should be active in the system
			if node1Alive && node2Alive {
				if nodeType[nodeId1] == NODE_CLIENT && nodeType[nodeId2] == NODE_CLIENT {
					return errors.New("cannot break connection between two clients")
				}
				/*
				Here goes the code to break the connection between two servers or between
				a server and a client.
				*/

				// If successful
				return nil
			}
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

	if node1Alive, ok1 := isNodeAlive[nodeId1]; ok1 {
		if node2Alive, ok2 := isNodeAlive[nodeId2]; ok2 {
			// Both nodes should be active in the system
			if node1Alive && node2Alive {
				if nodeType[nodeId1] == NODE_CLIENT && nodeType[nodeId2] == NODE_CLIENT {
					return errors.New("cannot create connection between two clients")
				}
				/*
				Here goes the code to create the connection between two servers or between
				a server and a client.
				*/

				// If successful
				return nil
			}
		}
	}
	return errors.New("either the nodes are not alive in the system or are not started yet")
}

func stabilize(args []string) error {
	/*
	Here goes the code to message all the servers to stabilize
	*/
	return nil
}

func printStore(args []string) error {
	serverId, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}

	// If the node with this id is not alive in the system
	if nodeAlive, ok := isNodeAlive[serverId]; !ok || !nodeAlive {
		return errors.New("Node with id " + strconv.Itoa(serverId) + " does not exist or is not alive!")
	}

	/*
	Here goes the code to tell the particular server to print it's key-value store
	*/

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
	if nodeAlive, ok := isNodeAlive[clientId]; !ok || !nodeAlive {
		return errors.New("Node with id " + strconv.Itoa(clientId) + " does not exist or is not alive!")
	}

	/*
	Here goes the code to tell the client to put the key-value pair.
	*/

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
	if nodeAlive, ok := isNodeAlive[clientId]; !ok || !nodeAlive {
		return errors.New("Node with id " + strconv.Itoa(clientId) + " does not exist or is not alive!")
	}

	/*
	Here goes the code to tell the client to print the value for the given key.
	*/

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

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		num_parts := len(parts)
		if num_parts == 0 {
			continue
		}
		if function, ok := functionMap[parts[0]]; ok {
			if num_parts - 1 != numArgsMap[parts[0]] {
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
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
