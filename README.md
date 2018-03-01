## Distributed key-value store
This is the implementation distributed key-value store. The key-value store provides eventual consistency plus two session guarantees: Read Your Writes and Monotonic Reads. There can be upto 5 servers and 5 clients communicating simultaneously in the kv-store with no shared memory, and these servers and clients are assumed to be different processes on a single machine. 

### Language used
The distributed key-value store has been implemented in Golang. 

### Dependencies
Golang should be installed in order to run the key-value store. 

### Compiling files
The program files can be compiled using the following commands:
	
	go build master.go
	go build server.go
	go build client.go

### Evaluating the key-value store
#### Creating the logs directory
A directory named 'logs' should be created in the root directory of the repository to store the logs of the clients and servers. 

#### Creating the commands file
The commands to be executed should be written in a text file, in the same format as that described in the project document [here](http://www.cs.utexas.edu/~vijay/cs380D-s18/project1.pdf). Each command should be written on a new line. An example commands file is commands.txt in the repository. 

#### Running the key-value store
The key-value store can be executed with the following command: 
	
	go run master.go commands.txt

#### Checking the output
The shell gives the outputs of the commands.
The server and client logs can be checked in the following files:
	
	logs/server[serverId].log
	logs/client[clientId].log

### Code Details
	
	The file master.go contains the code of the master process.
	The file server.go contains the code of the server process.
	The file client.go contains the code of the client process.
	The file util/util.go contains the common utilities like the logic for total ordering of events, creating RPC connections, etc.
	The file shared/shared_structs.go mostly contains the arguments that are passed during the RPC calls between different processes.
