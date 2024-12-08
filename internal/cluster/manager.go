package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/rs/zerolog"

	"github.com/DeltaLaboratory/shard/internal/storage"
)

const (
	defaultTimeout = 10 * time.Second
	joinTimeout    = 5 * time.Minute
)

type Manager struct {
	nodeID       string
	address      string
	Raft         *raft.Raft
	state        *State
	ring         *consistent.Consistent
	stateLock    sync.RWMutex
	clients      *ClientPool
	migrationMgr *MigrationManager
	store        *storage.PebbleStore

	logger zerolog.Logger
}

func NewManager(nodeID, address string, store *storage.PebbleStore, dataDir string, logger zerolog.Logger) (*Manager, error) {
	m := &Manager{
		nodeID:  nodeID,
		address: address,
		state:   &State{Nodes: make(map[string]*Node)},
		clients: NewClientPool(),
		store:   store,
		logger:  logger.With().Str("layer", "cluster").Logger(),
	}

	// Configure consistent hashing
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            &Hasher{},
	}
	m.ring = consistent.New(nil, cfg)

	// Initialize Raft
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	raftConfig.Logger = NewHclogAdapter(logger)

	// Configure Raft storage
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft-snapshots"), 3, nil)
	if err != nil {
		return nil, err
	}

	ipAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	// Create transport
	transport, err := raft.NewTCPTransport(address, ipAddr, 3, 10*time.Second, logger)
	if err != nil {
		return nil, err
	}

	m.migrationMgr = NewMigrationManager(m, store)
	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, &FSM{manager: m}, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	m.Raft = r
	return m, nil
}

func (m *Manager) NodeID() string {
	return m.nodeID
}

func (m *Manager) IsLeader() bool {
	return m.Raft.State() == raft.Leader
}

func (m *Manager) GetLeader() (string, error) {
	leaderAddr, leaderId := m.Raft.LeaderWithID()
	if leaderAddr == "" {
		return "", fmt.Errorf("no leader")
	}
	return string(leaderId), nil
}

func (m *Manager) WaitForLeader(timeout time.Duration) error {
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return fmt.Errorf("timeout waiting for leader")
		case <-ticker.C:
			if leader, _ := m.GetLeader(); leader != "" {
				return nil
			}
		}
	}
}

// Bootstrap creates a new cluster
func (m *Manager) Bootstrap() error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(m.nodeID),
				Address: raft.ServerAddress(m.address),
			},
		},
	}
	return m.Raft.BootstrapCluster(configuration).Error()
}

// Join adds a new node to the cluster
func (m *Manager) Join(nodeID, raftAddress, rpcAddress string) error {
	if m.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	m.logger.Info().Str("node_id", nodeID).Str("raft_address", raftAddress).Str("rpc_address", rpcAddress).Msg("Joining cluster")

	futureConfig := m.Raft.GetConfiguration()
	if err := futureConfig.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}

	for _, server := range futureConfig.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			m.logger.Info().Str("node_id", nodeID).Msg("Node already exists in the cluster")
			return nil
		}
	}

	// Step 1: Add node in JOINING state
	newNode := &Node{
		ID:          nodeID,
		RaftAddress: raftAddress,
		RPCAddress:  rpcAddress,
		State:       NodeStateJoining,
		HashKey:     Hasher{}.Sum64([]byte(nodeID)),
	}

	// Verify connection to the new node
	_, err := m.clients.GetClient(nodeID, rpcAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to new node: %v", err)
	}

	m.logger.Info().Str("node_id", nodeID).Str("raft_address", raftAddress).Str("rpc_address", rpcAddress).Msg("Connected to new node")

	m.logger.Info().Str("node_id", nodeID).Msg("Adding node to raft")
	// Add node to raft
	f := m.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddress), 0, 0)
	if err := f.Error(); err != nil {
		m.clients.RemoveClient(nodeID)
		return fmt.Errorf("failed to add node to raft: %v", err)
	}
	m.logger.Info().Str("node_id", nodeID).Msg("Added node to raft")

	m.logger.Info().Str("node_id", nodeID).Msg("Creating migration plan")

	// Step 2: Create and apply migration plan
	oldRing := m.ring
	newNodes := append(m.GetActiveNodes(false), newNode)
	newRing := m.createNewRing(newNodes)

	plan, err := m.CreateMigrationPlan(oldRing, newRing)
	if err != nil {
		m.clients.RemoveClient(nodeID)
		return fmt.Errorf("failed to create migration plan: %v", err)
	}

	m.logger.Info().Str("node_id", nodeID).Msg("Created migration plan")

	// Step 3: Apply node addition and migration plan through Raft
	cmd := &Command{
		Op:            CommandJoinNode,
		Node:          newNode,
		MigrationPlan: plan,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		m.clients.RemoveClient(nodeID)
		return fmt.Errorf("failed to marshal join command: %v", err)
	}

	// Apply command through Raft
	if err := m.Raft.Apply(data, defaultTimeout).Error(); err != nil {
		m.clients.RemoveClient(nodeID)
		return fmt.Errorf("failed to apply join command: %v", err)
	}

	m.logger.Info().Str("node_id", nodeID).Msg("Applied join command")

	// Step 4: Execute migrations
	ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
	defer cancel()

	if err := m.executeMigrations(ctx, plan); err != nil {
		// If migration fails, we need to rollback
		rollbackCmd := &Command{
			Op:   CommandRemoveNode,
			Node: newNode,
		}
		rollbackData, _ := json.Marshal(rollbackCmd)
		m.Raft.Apply(rollbackData, defaultTimeout)
		m.clients.RemoveClient(nodeID)
		return fmt.Errorf("migration failed: %v", err)
	}

	m.logger.Info().Str("node_id", nodeID).Msg("Executed migrations")

	// Step 5: Update node state to ACTIVE
	updateCmd := &Command{
		Op: CommandUpdateNodeState,
		Node: &Node{
			ID:    nodeID,
			State: NodeStateActive,
		},
	}

	updateData, _ := json.Marshal(updateCmd)
	if err := m.Raft.Apply(updateData, defaultTimeout).Error(); err != nil {
		return fmt.Errorf("failed to activate node: %v", err)
	}

	m.logger.Info().Str("node_id", nodeID).Msg("Activated node")

	return nil
}

// Leave removes a node from the cluster
func (m *Manager) Leave(nodeID string) error {
	if m.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	// Step 1: Update node state to LEAVING
	updateCmd := &Command{
		Op: CommandUpdateNodeState,
		Node: &Node{
			ID:    nodeID,
			State: NodeStateLeaving,
		},
	}

	updateData, _ := json.Marshal(updateCmd)
	if err := m.Raft.Apply(updateData, defaultTimeout).Error(); err != nil {
		return fmt.Errorf("failed to update node state: %v", err)
	}

	// Step 2: Create migration plan
	oldRing := m.ring
	newNodes := make([]*Node, 0)
	for _, node := range m.GetActiveNodes(false) {
		if node.ID != nodeID {
			newNodes = append(newNodes, node)
		}
	}
	newRing := m.createNewRing(newNodes)

	plan, err := m.CreateMigrationPlan(oldRing, newRing)
	if err != nil {
		return fmt.Errorf("failed to create migration plan: %v", err)
	}

	// Step 3: Apply migration plan through Raft
	cmd := &Command{
		Op:            CommandLeaveNode,
		Node:          &Node{ID: nodeID},
		MigrationPlan: plan,
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal leave command: %v", err)
	}

	if err := m.Raft.Apply(data, defaultTimeout).Error(); err != nil {
		return fmt.Errorf("failed to apply leave command: %v", err)
	}

	// Step 4: Execute migrations
	ctx, cancel := context.WithTimeout(context.Background(), joinTimeout)
	defer cancel()

	if err := m.executeMigrations(ctx, plan); err != nil {
		return fmt.Errorf("migration failed: %v", err)
	}

	// Step 5: Remove node from cluster
	removeCmd := &Command{
		Op:   CommandRemoveNode,
		Node: &Node{ID: nodeID},
	}

	removeData, _ := json.Marshal(removeCmd)
	if err := m.Raft.Apply(removeData, defaultTimeout).Error(); err != nil {
		return fmt.Errorf("failed to remove node: %v", err)
	}

	// Clean up client connection
	m.clients.RemoveClient(nodeID)

	return nil
}

// GetNode returns the responsible node for a given key
func (m *Manager) GetNode(key []byte) (*Node, error) {
	m.stateLock.RLock()
	defer m.stateLock.RUnlock()

	member := m.ring.LocateKey(key)
	if member == nil {
		return nil, fmt.Errorf("no node found for key")
	}

	node, exists := m.state.Nodes[member.String()]
	if !exists {
		return nil, fmt.Errorf("node not found")
	}

	return node, nil
}

// UpdateState updates the cluster state and ring
func (m *Manager) UpdateState(state *State) {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	m.state = state

	// Update consistent hash ring
	m.ring = consistent.New(nil, consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            &Hasher{},
	})

	for _, node := range state.Nodes {
		if node.State == NodeStateActive {
			m.ring.Add(node)
		}
	}
}

// Create new consistent hash ring with given nodes
func (m *Manager) createNewRing(nodes []*Node) *consistent.Consistent {
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            &Hasher{},
	}

	ring := consistent.New(nil, cfg)
	for _, node := range nodes {
		ring.Add(node)
	}

	return ring
}

func (m *Manager) GetNodeByID(nodeID string) (*Node, error) {
	m.stateLock.RLock()
	defer m.stateLock.RUnlock()

	node, exists := m.state.Nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found")
	}

	return node, nil
}

func (m *Manager) GetActiveNodes(noLock bool) []*Node {
	m.logger.Debug().Msg("GetActiveNodes")

	if !noLock {
		m.stateLock.RLock()
		defer m.stateLock.RUnlock()
	}

	var activeNodes []*Node
	for _, node := range m.state.Nodes {
		if node.State == NodeStateActive {
			activeNodes = append(activeNodes, node)
		}
	}

	m.logger.Debug().Msg("GetActiveNodes: done")
	return activeNodes
}

func (m *Manager) executeMigrations(ctx context.Context, plan *MigrationPlan) error {
	for _, task := range plan.Tasks {
		// Skip if this node is not involved in the task
		if task.SourceNode != m.nodeID {
			continue
		}

		if err := m.migrationMgr.ExecuteMigrationTask(ctx, task); err != nil {
			return fmt.Errorf("failed to execute migration task %s: %v", task.ID, err)
		}

		// Update task state through Raft
		updateCmd := &Command{
			Op: CommandUpdateMigrationTask,
			MigrationTask: &MigrationTask{
				ID:    task.ID,
				State: MigrationCompleted,
			},
		}

		data, _ := json.Marshal(updateCmd)
		if err := m.Raft.Apply(data, defaultTimeout).Error(); err != nil {
			return fmt.Errorf("failed to update migration state: %v", err)
		}
	}

	return nil
}

func (m *Manager) Close() error {
	m.clients.Close()
	if m.Raft != nil {
		return m.Raft.Shutdown().Error()
	}
	return nil
}
