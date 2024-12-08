package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lesismal/arpc"

	"github.com/DeltaLaboratory/dkv/internal/protocol"
)

type ClientPool struct {
	clients map[string]*PooledClient
	mu      sync.RWMutex
}

type PooledClient struct {
	client  *arpc.Client
	address string
	mu      sync.Mutex // for exclusive client usage during batch operations
}

func NewClientPool() *ClientPool {
	return &ClientPool{
		clients: make(map[string]*PooledClient),
	}
}

func (p *ClientPool) GetClient(nodeID, address string) (*PooledClient, error) {
	p.mu.RLock()
	pc, exists := p.clients[nodeID]
	p.mu.RUnlock()

	if exists {
		// Check if address changed
		if pc.address != address {
			p.mu.Lock()
			// Double check after acquiring write lock
			if pc, exists = p.clients[nodeID]; exists && pc.address != address {
				pc.client.Stop()
				delete(p.clients, nodeID)
			}
			p.mu.Unlock()
		} else {
			return pc, nil
		}
	}

	// Create new client
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check after lock
	if pc, exists = p.clients[nodeID]; exists {
		return pc, nil
	}

	client, err := arpc.NewClient(func() (net.Conn, error) {
		return net.Dial("tcp", address)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client for node %s: %v", nodeID, err)
	}

	pc = &PooledClient{
		client:  client,
		address: address,
	}
	p.clients[nodeID] = pc

	return pc, nil
}

func (p *ClientPool) RemoveClient(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, exists := p.clients[nodeID]; exists {
		pc.client.Stop()
		delete(p.clients, nodeID)
	}
}

func (p *ClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, pc := range p.clients {
		pc.client.Stop()
	}
	p.clients = make(map[string]*PooledClient)
}

// BatchSet implementation for pooled client
func (pc *PooledClient) BatchSet(ctx context.Context, batch map[string][]byte) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	req := &protocol.BatchKVRequest{
		Batch: batch,
	}

	var resp protocol.KVResponse
	err := pc.client.Call("/kv/batch-set", req, &resp, time.Minute*5)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf(resp.Error)
	}

	return nil
}
