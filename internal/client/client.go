package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/lesismal/arpc"
	"github.com/rs/zerolog"

	"github.com/DeltaLaboratory/shard/internal/protocol"
)

type Client struct {
	// Connection pool for different servers
	clients     map[string]*arpc.Client
	clientsLock sync.RWMutex

	// Consistent hashing
	ring     *consistent.Consistent
	ringLock sync.RWMutex

	// Leader connection for cluster info
	leaderClient *arpc.Client
	leaderAddr   string

	timeout time.Duration
	logger  zerolog.Logger
}

func NewClient(bootStrapAddr string, logger ...zerolog.Logger) (*Client, error) {
	bootStrapClient, err := arpc.NewClient(func() (net.Conn, error) {
		return net.Dial("tcp", bootStrapAddr)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader: %v", err)
	}

	c := &Client{
		clients:      make(map[string]*arpc.Client),
		leaderClient: bootStrapClient,
		leaderAddr:   bootStrapAddr,
		timeout:      5 * time.Second,
	}

	if len(logger) > 0 {
		c.logger = logger[0].With().Str("layer", "client").Logger()
	} else {
		c.logger = zerolog.Nop()
	}

	// Initial cluster info fetch
	if err := c.updateClusterInfo(); err != nil {
		bootStrapClient.Stop()
		return nil, err
	}

	// Start periodic cluster info updates
	go c.periodicClusterUpdate()

	return c, nil
}

func (c *Client) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	_, client, err := c.getServerForKey(key)
	if err != nil {
		return nil, err
	}

	req := &protocol.KVRequest{
		Op:  protocol.OpGet,
		Key: key,
	}

	var resp protocol.KVResponse
	err = client.Call("/kv/get", req, &resp, c.timeout)
	if err != nil {
		return nil, err
	}

	if resp.RedirectTo != "" {
		// Handle redirect
		return c.redirect(ctx, resp.RedirectTo, req)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf(resp.Error)
	}

	return resp.Value, nil
}

func (c *Client) Set(ctx context.Context, key, value []byte) error {
	_, client, err := c.getServerForKey(key)
	if err != nil {
		return err
	}

	req := &protocol.KVRequest{
		Op:    protocol.OpSet,
		Key:   key,
		Value: value,
	}

	var resp protocol.KVResponse
	err = client.Call("/kv/set", req, &resp, c.timeout)
	if err != nil {
		return err
	}

	if resp.RedirectTo != "" {
		// Handle redirect
		_, err = c.redirect(ctx, resp.RedirectTo, req)
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, client, err := c.getServerForKey(key)
	if err != nil {
		return err
	}

	req := &protocol.KVRequest{
		Op:  protocol.OpDelete,
		Key: key,
	}

	var resp protocol.KVResponse
	err = client.Call("/kv/delete", req, &resp, c.timeout)
	if err != nil {
		return err
	}

	if resp.RedirectTo != "" {
		// Handle redirect
		_, err = c.redirect(ctx, resp.RedirectTo, req)
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (c *Client) redirect(ctx context.Context, serverAddr string, req *protocol.KVRequest) ([]byte, error) {
	// Create temporary client for redirect
	client, err := arpc.NewClient(func() (net.Conn, error) {
		return net.Dial("tcp", serverAddr)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to redirect server: %v", err)
	}
	defer client.Stop()

	var resp protocol.KVResponse
	err = client.Call("/kv/redirect", req, &resp, c.timeout)
	if err != nil {
		return nil, err
	}

	if resp.Error != "" {
		return nil, fmt.Errorf(resp.Error)
	}

	return resp.Value, nil
}

func (c *Client) BatchSet(ctx context.Context, nodeID string, batch map[string][]byte) error {
	req := &protocol.BatchKVRequest{
		Batch: batch,
	}

	c.clientsLock.RLock()
	client, exists := c.clients[nodeID]
	c.clientsLock.RUnlock()

	if !exists {
		return fmt.Errorf("no connection to server %s", nodeID)
	}

	var resp protocol.KVResponse
	err := client.Call("/kv/batchset", req, &resp, c.timeout)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}

func (c *Client) updateLeaderConnection() error {
	var resp protocol.ServerInfo
	err := c.leaderClient.Call("/cluster/leader", struct{}{}, &resp, c.timeout)
	if err != nil {
		return fmt.Errorf("failed to get leader info: %v", err)
	}

	if resp.RaftAddress == "" {
		return fmt.Errorf("no leader available")
	}

	if c.leaderAddr == resp.RPCAddress {
		return nil
	}

	// Reuse existing connection or create new one
	c.clientsLock.RLock()
	client, exists := c.clients[resp.ID]
	c.clientsLock.RUnlock()

	if !exists {
		client, err = arpc.NewClient(func() (net.Conn, error) {
			return net.Dial("tcp", resp.RPCAddress)
		})
		if err != nil {
			return err
		}
	}

	c.clientsLock.Lock()
	c.clients[resp.ID] = client
	c.clientsLock.Unlock()

	c.leaderAddr = resp.RPCAddress
	c.leaderClient = client

	return nil
}

func (c *Client) updateClusterInfo() error {
	var resp protocol.ClusterInfoResponse
	err := c.leaderClient.Call("/cluster/info", struct{}{}, &resp, c.timeout)
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %v", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("cluster info error: %s", resp.Error)
	}

	// Update consistent hash ring
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            &Hasher{}, // Same hasher as server
	}

	newRing := consistent.New(nil, cfg)

	// Update connections pool
	newClients := make(map[string]*arpc.Client)

	for _, server := range resp.Servers {
		newRing.Add(&ServerNode{
			ID:          server.ID,
			RaftAddress: server.RaftAddress,
			RPCAddress:  server.RPCAddress,
		})

		// Reuse existing connection or create new one
		c.clientsLock.RLock()
		client, exists := c.clients[server.ID]
		c.clientsLock.RUnlock()

		if !exists {
			client, err = arpc.NewClient(func() (net.Conn, error) {
				return net.Dial("tcp", server.RPCAddress)
			})
			if err != nil {
				continue // Skip failed connections
			}
		}

		newClients[server.ID] = client
	}

	// Update ring and clients atomically
	c.ringLock.Lock()
	c.ring = newRing
	c.ringLock.Unlock()

	c.clientsLock.Lock()
	// Close removed connections
	for id, client := range c.clients {
		if _, exists := newClients[id]; !exists {
			client.Stop()
		}
	}
	c.clients = newClients
	c.clientsLock.Unlock()

	return nil
}

func (c *Client) periodicClusterUpdate() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := c.updateLeaderConnection(); err != nil {
			c.logger.Warn().Err(err).Msg("failed to update leader connection")
			continue
		}

		if err := c.updateClusterInfo(); err != nil {
			c.logger.Warn().Err(err).Msg("failed to update cluster info")
			continue
		}
	}
}

func (c *Client) getServerForKey(key []byte) (string, *arpc.Client, error) {
	c.ringLock.RLock()
	defer c.ringLock.RUnlock()

	if c.ring == nil {
		return "", nil, fmt.Errorf("ring not initialized")
	}

	member := c.ring.LocateKey(key)
	if member == nil {
		return "", nil, fmt.Errorf("no server available for key")
	}

	serverNode := member.(*ServerNode)

	c.clientsLock.RLock()
	client, exists := c.clients[serverNode.ID]
	c.clientsLock.RUnlock()

	if !exists {
		return "", nil, fmt.Errorf("no connection to server %s", serverNode.ID)
	}

	return serverNode.ID, client, nil
}

func (c *Client) Close() error {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	for _, client := range c.clients {
		client.Stop()
	}

	if c.leaderClient != nil {
		c.leaderClient.Stop()
	}

	return nil
}

type ServerNode struct {
	ID          string
	RaftAddress string
	RPCAddress  string
}

func (n *ServerNode) String() string {
	return n.ID
}

type Hasher struct{}

func (h Hasher) Sum64(data []byte) uint64 {
	var hash uint64 = 14695981039346656037
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211
	}
	return hash
}
