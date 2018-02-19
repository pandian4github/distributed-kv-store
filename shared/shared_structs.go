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
