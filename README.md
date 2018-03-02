## Distributed key-value store
This is the implementation distributed key-value store. The key-value store provides eventual consistency plus two session guarantees: Read Your Writes and Monotonic Reads. There can be many servers and clients communicating simultaneously in the kv-store with no shared memory between them, and these servers and clients are assumed to be different processes running on a single machine although it is easily extensible to run them on different machines. 

### Contributors
	Pandian Raju (UT EID: pr22695, UTCS ID: pandian)
	Soujanya Ponnapalli (UT EID: sp43596, UTCS ID: soujanya)
	Rohan Kadekodi (UT EID: rak2575, UTCS ID: rak)

### Language used
The distributed key-value store has been implemented in Golang. 

### Dependencies
Golang should be installed in order to run the key-value store. The version of Golang for which the key-value store is guaranteed to work is go1.10 although it also works on go1.6.2 (the default version of CS public servers) 

### Compiling files
The program files can be compiled using the following commands:
	
	go build master.go
	go build server.go
	go build client.go

### Evaluating the key-value store
#### Creating the commands file
The commands to be executed should be written in a text file, in the same format as that described in the project document [here](http://www.cs.utexas.edu/~vijay/cs380D-s18/project1.pdf). Each command should be written on a new line. An example commands file is commands.txt in the repository. 

#### Running the key-value store
The key-value store can be executed with the following command: 
	
	go run master.go <path_to_commands_file> 
	for example: go run master.go commands.txt

#### Checking the output
The stdout gives the output of the master process and the output of the commands like get or printStore.
The server and client logs contain the outputs of the respective server or client and can be checked in the following files:
	
	logs/server[serverId].log
	logs/client[clientId].log
	
For ease of checking the logs, redirect the entire log to some file and grep for CMD_OUTPUT to check only the output of the commands. You can also grep for THROUGHPUT to look at the timer information. For example, to just look at these two sets of logs, 
    
    go run master.go commands.txt &> out.log
    cat out.log | grep -e CMD_OUTPUT -e THROUGHPUT  

#### Test cases
We have written a suite of test cases for which the key-value store has been tested. The test cases are in the folder named 'test_cases'.

#### Benchmarking
At the end of the master program execution, throughput information for the individual types of operations as well as the overall throughput will be displayed in stdout. 

#### Killing running processes
The master program has been set to kill all the clients and servers that it started, at the end. But if any processes are still running, the scripts under scripts/ folder can be used to kill them.
    
    cd scripts
    ./kill_all_processes.sh

### Implementation Details
	
	The file master.go contains the code of the master process.
	The file server.go contains the code of the server process.
	The file client.go contains the code of the client process.
	The file util/util.go contains the common utilities like the logic for total ordering of events, creating RPC connections, etc.
	The file shared/shared_structs.go mostly contains the arguments that are passed during the RPC calls between different processes.

#### How are the key-value pairs stored?  
The server processes do not share memory. Therefore, the key-value store is replicated in all the servers. The key-value store is basically a hashmap from key to value, stored in the memory of each server process. At any server, there are two hashmaps maintained: 
* the key-value pairs that are not yet communicated to other servers (new insertions from the last stabilize) are stored in 'InFlightDB'; 
* the key-value pairs that have been communicated to all the other connected servers (all data inserted until the last stabilize) are stored in 'PersistedDB'.

PersistedDB is consistent across all the servers at any point in time while InFlightDB may vary between servers. During each stabilize command, the InFlightDBs of all the servers are communicated with each other, and the key-value pairs from the InFlightDB are stored in the PersistedDB by each server. At each server, the InFlightDB or PersistedDB cannot have more than one value for a single key. 

Total ordering (persist value from lower server id, if concurrent) is issued to ensure that only the most recent value of a particular key is stored in InFlightDB and PersistedDB. However, there can be multiple values for a single key across servers in their InFlightDBs, as the InFlightDB contains the key-value pairs that are not yet communicated between servers. 

#### Synchronization between nodes
To provide eventual consistency and the two session guarantees, the key-value store uses vector clocks. The vector clock is a map of serverId (or clientId) versus a logical counter (logical clock of that server or client). Each event at a server or client causes its logical counter to be incremented. 

#### Starting a server
The joinServer command starts a new server process (sends the list of existing servers to this server) and also sends this new server information to all the existing servers. It also does a bootstrap request call from a random connected server to retrieve the PersistedDB. After the bootstrap, server spawns three threads: to listen to the master (to execute cluster commands), clients (to execute get/put commands) and other servers (stabilize) respectively. 

#### Starting a client
The joinClient command starts a new client process and sends the server information to connect to the client. The client spawns a thread which listens to the master (to execute the commands). There can be multiple servers connected with a client. During a put or a get request issued to the client, the connected server with the lowest serverId receives the request. 

#### Put implementation
When a put(key, value) command is executed, the server which receives the request stores the key-value pair along with its vector timestamp and the serverId in the InFlightDB, and returns the vector timestamp to the client. The client maintains a history of each inserted key versus its corresponding server vector timestamp and the serverId to which the key was inserted. The client history is used for the two session guarantees, namely Read-Your-Writes and Monotonic Reads.

#### Get implementation
When a get(key) command is executed, the server which receives the request looks up the key first in its InFlightDB and then in the PersistedDB, and either returns the <value, timestamp, serverId> tuple of the key or returns 'ERR_NO_KEY' if it does not find the key in both the DBs. 

In case the <value, timestamp, serverId> for the key is returned by the server, the client checks the returned <timestamp, serverId> with the <timestamp, serverId> for that key in its history. If the key is not present in the client history, the client adds the <key: timestamp, serverId> entry to its history and returns the value. If the key is present in the client history (which means the client had previously accessed the key during a get or put), the <timestamp, serverId> for the key in the client history is compared with the <timestamp, serverId> of the key returned by the server, and based on total ordering, either the value or 'ERR_DEP' is printed by the client. If the client history timestamp and the timestamp returned by the server are concurrent, total ordering is ensured based on the serverId (lower serverId wins). 

* If the timestamp returned by the server strictly happened before any of the timestamps for the key in the client history, the <timestamp and serverId> is not stored in the client history and 'ERR_DEP' is printed. 
* If the timestamp returned by the server strictly happened after any of the timestamps for the key in the client history, the corresponding timestamps and serverIds are removed from the client history for that key and the new <timestamp, serverId> is stored in the client history. 
* If the timestamp returned by the server is concurrent with any of the timestamps for the key in the client history, the <timestamp, serverId> is stored in the client history.

#### Stabilize
The stabilize command results in the exchange of InFlightDBs of all the connected servers, and storing of the InFlightDBs in the PersistedDBs. At the end of stabilize, the InFlightDBs of all the servers are empty, and the PersistedDBs of all the servers reflect the same database.  

During stabilize, each server first propagates its own InFlightDB along with the IDs of its neighbours to all of its directly connected servers. Then it does transitive propagation, where it propagates the InFlightDB received from one server 'X' to its neighbor 'Y' if it sees that 'X' and 'Y' are not connected directly. This is done at all servers, and it is done until the point that all the servers have received the InFlightDBs of all the other directly and indirectly connected servers. An important thing to note is that the servers do not know the entire network topology (since the master doesn't store this state), rather it gets to know the topology as it keeps sending and receiving transitive updates.

During a network partition, the stabilize command will makes the nodes within a partition consistent. When a connection is made between two servers (which connects two partitions), the servers initially exchange their persistedDBs with each other (which are added to the inFlightDB of the servers) since they will be having different persistedDBs after their previous stabilizes. This ensures that during the next stabilize command, all the servers within the network will become consistent and end up with the same copy of persistedDB.  
 
#### Killing a server
Killing a server sends a kill command to the corresponding server (which will then exit) and also notifies the other servers about the killed server so that they can remove the server from the cluster. It also notifies the clients which were connected to that server about the server being killed.

#### Creating or breaking connection
Creating or breaking connection between two servers notifies both servers about the change while doing the same between a client and a server notifies just the client about the change (since server doens't maintain open connections with clients but vice-versa). 
