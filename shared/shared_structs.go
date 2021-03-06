package shared

/*
This file contains the shared structs for request and response used in different
RPC calls between master, server and clients.
*/

// Argument to the other servers when a new server joins the cluster
type NewServerArgs struct {
	ServerId int
	HostPortPair string // host:port
}

// Argument to the other servers when a new server joins the cluster
type ClientServerConnectionArgs struct {
	ServerId int
	ServerBasePort int
}

type RemoveServerArgs struct {
	ServerId int
}

type MasterToClientPutArgs struct {
	Key, Value string
}

type ClientToServerPutArgs struct {
	Key, Value string
	ClientId int
	ClientClock Clock
}

type ServerToClientGetReply struct {
	Value Value
	ServerVecTs Clock
}

type ClientToServerGetArgs struct {
	Key string
	ClientVecTs Clock
}

/*
Value is the struct which encloses the actual value stored in the key-value store
*/
type Value struct {
	Val string // actual value
	Ts map[int]int // vector timestamp
	ServerId int // server to which the value was written
	ClientId int // client which wrote this value
}

type BootstrapDataResponse struct {
	VecTs Clock // clock time of that server
	PersistedDb map[string]Value
}

type StabilizeDbRequest struct {
	ServerId int
	VecTs Clock
	InFlightDB map[string]Value
}

type StabilizeDataPacket struct {
	VecTs Clock
	InFlightDB map[string]Value
	Peers map[int]string // otherServers details of ServerId
}

type StabilizeDataPackets map[int]StabilizeDataPacket

type SendDataPacketsRequest struct {
	ServerId int
	DataPackets StabilizeDataPackets
}

type SyncStabilizeCheckpointRequest struct {
	ServerId int
	StabilizeCheckpoint map[int]int
}

type SyncStabilizeCheckpointResponse struct {
	NotInSync bool
	ServerId int
	VecTs Clock
	PersistedDB map[string]Value
}

type Clock map[int]int
