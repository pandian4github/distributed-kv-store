## Distributed key-value store
This is the implementation distributed key-value store. The key-value store provides eventual consistency plus two session guarantees: Read Your Writes and Monotonic Reads. There can be upto 5 servers and 5 clients communicating simultaneously in the kv-store with no shared memory, and these servers and clients are assumed to be different processes on a single machine. 

### Contributors
	Pandian Raju (UT EID: pr22695, UTCS ID: pandian)
	Soujanya Ponnapalli (UT EID: sp43596, UTCS ID: soujanya)
	Rohan Kadekodi (UT EID: rak2575, UTCS ID: rak)

### Language used
The distributed key-value store has been implemented in Golang. 

### Dependencies
Golang should be installed in order to run the key-value store. The version of Golang for which the key-value store is guaranteed to work is: go1.10 darwin/amd64

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
	
	go run master.go commands.txt

#### Checking the output
The shell gives the outputs of the master process.
The server and client logs contain the outputs of the respective server or client and can be checked in the following files:
	
	logs/server[serverId].log
	logs/client[clientId].log

#### Test cases
We have written a suite of test cases for which the key-value store has been tested. The test cases are in the folder named 'test_cases'

### Implementation Details
	
	The file master.go contains the code of the master process.
	The file server.go contains the code of the server process.
	The file client.go contains the code of the client process.
	The file util/util.go contains the common utilities like the logic for total ordering of events, creating RPC connections, etc.
	The file shared/shared_structs.go mostly contains the arguments that are passed during the RPC calls between different processes.

#### key-value store design
The server processes do not share memory. Therefore, the key-value store is replicated at all the processes. The key-value store is basically a hashmap from key to value, stored in memory of each server process. The key-value pairs that are not communicated by a server with the other servers are stored in the InFlightDB. The key-value pairs that have been communicated by a server to all the other connected servers are stored in the PersistentDB. The PersistentDB is consistent across all the servers while the InFlightDB may vary between servers. At each stabilize command, the InFlightDBs of all the servers are communciated with each other, and the key-value pairs from the InFlightDB are stored in the PersistentDB by each server. At each server, the InFlightDB or PersistentDB cannot have more than one value for a single key. The total ordering is issued to ensure that only the most recent value of a particular key is stored in the InFlightDB and the PersistentDB. However, there can be multiple values for a single key across servers in their InFlightDBs, as the InFlightDB contains the key-value pairs that are not communicated between servers. 

#### Clock
For maintaining eventual consistency and the two session guarantees, the key-value store uses vector clocks. The vector clock is basically a map of serverId(or clientId) versus a logical counter. Each event at a server or client causes its logical counter to be incremented. 

#### Starting a server or a client
The joinServer command creates a new server process, which, on getting created, sends its information to all the other servers and receives information from all the other servers, thus forming a connection with all of them. After all this is done, the server starts listening to the master for commands. A joinClient results in creating a client process and making a connection of the client process with the server process specified by the serverId in the command. The client, on connecting with the server, starts listening to the master. There can be multiple servers connected with a client. On a put or a get request from the client, the connected server with the lowest serverId receives the request. 

#### Put implementation
When a put(key, value) command is executed, the server which receives the request stores the key-value pair along with its vector timestamp and the serverId in the InFlightDB. Also, the client maintains a history of each inserted key versus its corresponding server vector timestamp and the serverId that inserted the key. The client history is used for the two session guarantees, namely Read-Your-Writes and Monotic Reads.

#### Get implementation
When a get(key) command is executed, the server which receives the request looks up the key first in the InFlightDB and then in the PersistentDB, and either returns the value, timestamp, serverId tuple of the key or returns 'ERR_NO_KEY' if it does not find the key in both the DBs. In case the value, timestamp, serverId for the key is returned by the server, the client checks the returned timestamp, serverId with the timestamp, serverId for that key in its history. If the key is not present in the client history, the client adds the key, timestamp, serverId tuple to its history and returns the value. If the key is present in the client history, meaning that the client had previously accessed the key, the timestamp, serverId for the key in the client history is compared with the timestamp, serverId of the key returned by the server, and based on total ordering, either the value or 'ERR_DEP' is returned by the client. If the client history timestamp and the timestamp returned by the server are concurrent, total ordering is ensured based on the serverId, in which the lower serverId wins. If the timestamp returned by the server is concurrent with any of the timestamps for the key in the client history, the timestamp and serverId is stored in the client history. If the timestamp returned by the server strictly happened before all the timestamps for the key in the client history, the timestamp and serverId is not stored in the client history. If the timestamp returned by the server strictly happened after any of the timestamps for the key in the client history, the corresponding timestamps and serverIds are removed from the client history for that key and the new timestamp, serverId is stored in the client history. 

#### Stabilize
The stabilize command results in the exchange of InFlightDBs of all the connected servers, and storing of the InFlightDBs in the PersistentDBs. At the end of stabilize, the InFlightDBs of all the servers are empty, and the PersistentDBs of all the servers reflect the same database. During stabilize, each server first propogates its own InFlightDB along with the IDs of its neighbours to all of its directly connected servers. Then it does transitive propogation, where it propogates the InFlightDB received from one server to another server when the two are not connected to each other directly, and are connected to this server. This is done at all servers, and it is done until the point that all the servers have received the InFlightDBs of all the other directly and indirectly connected servers. 





