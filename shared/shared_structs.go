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

type RemoveServerArgs struct {
	ServerId int
}

type PutArgs struct {
	Key, Value string
	ClientId, ServerId, ClientClock int
}

type GetArgs struct {
	Key string
	ClientId, ServerId, ClientClock int
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

type Clock map[int]int
