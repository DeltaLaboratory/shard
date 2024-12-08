package cluster

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

type CommandType uint8

const (
	CommandJoinNode CommandType = iota
	CommandLeaveNode
	CommandUpdateNodeState
	CommandRemoveNode
	CommandUpdateMigrationTask
)

type Command struct {
	Op            CommandType    `json:"op"`
	Node          *Node          `json:"node,omitempty"`
	MigrationPlan *MigrationPlan `json:"migration_plan,omitempty"`
	MigrationTask *MigrationTask `json:"migration_task,omitempty"`
}

type FSM struct {
	manager *Manager
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.manager.stateLock.Lock()
	defer f.manager.stateLock.Unlock()

	switch cmd.Op {
	case CommandJoinNode:
		return f.applyJoinNode(cmd)
	case CommandLeaveNode:
		return f.applyLeaveNode(cmd)
	case CommandUpdateNodeState:
		return f.applyUpdateNodeState(cmd)
	case CommandRemoveNode:
		return f.applyRemoveNode(cmd)
	case CommandUpdateMigrationTask:
		return f.applyUpdateMigrationTask(cmd)
	default:
		return fmt.Errorf("unknown command type: %v", cmd.Op)
	}
}

func (f *FSM) applyJoinNode(cmd Command) interface{} {
	if cmd.Node == nil {
		return fmt.Errorf("node data is missing")
	}

	// Add new node to cluster state
	f.manager.state.Nodes[cmd.Node.ID] = cmd.Node

	// If there's a migration plan, initialize or update it
	if cmd.MigrationPlan != nil {
		f.manager.state.Migrations = cmd.MigrationPlan
	}

	// Only update ring if node is active
	if cmd.Node.State == NodeStateActive {
		nodes := f.manager.GetActiveNodes()
		f.manager.ring = f.manager.createNewRing(nodes)
	}

	return nil
}

func (f *FSM) applyLeaveNode(cmd Command) interface{} {
	if cmd.Node == nil {
		return fmt.Errorf("node data is missing")
	}

	// Update node state to leaving
	if node, exists := f.manager.state.Nodes[cmd.Node.ID]; exists {
		node.State = NodeStateLeaving
	} else {
		return fmt.Errorf("node %s not found", cmd.Node.ID)
	}

	// Store migration plan
	if cmd.MigrationPlan != nil {
		f.manager.state.Migrations = cmd.MigrationPlan
	}

	// Update ring excluding leaving node
	nodes := f.manager.GetActiveNodes()
	f.manager.ring = f.manager.createNewRing(nodes)

	return nil
}

func (f *FSM) applyUpdateNodeState(cmd Command) interface{} {
	if cmd.Node == nil {
		return fmt.Errorf("node data is missing")
	}

	node, exists := f.manager.state.Nodes[cmd.Node.ID]
	if !exists {
		return fmt.Errorf("node %s not found", cmd.Node.ID)
	}

	oldState := node.State
	node.State = cmd.Node.State

	// If state changed to/from active, update ring
	if (oldState != NodeStateActive && cmd.Node.State == NodeStateActive) ||
		(oldState == NodeStateActive && cmd.Node.State != NodeStateActive) {
		nodes := f.manager.GetActiveNodes()
		f.manager.ring = f.manager.createNewRing(nodes)
	}

	return nil
}

func (f *FSM) applyRemoveNode(cmd Command) interface{} {
	if cmd.Node == nil {
		return fmt.Errorf("node data is missing")
	}

	// Remove node from cluster state
	delete(f.manager.state.Nodes, cmd.Node.ID)

	// Update ring
	nodes := f.manager.GetActiveNodes()
	f.manager.ring = f.manager.createNewRing(nodes)

	return nil
}

// internal/cluster/fsm.go
func (f *FSM) applyUpdateMigrationTask(cmd Command) interface{} {
	if cmd.MigrationTask == nil {
		return fmt.Errorf("migration task data is missing")
	}

	if f.manager.state.Migrations == nil {
		return fmt.Errorf("no active migration plan")
	}

	// Update migration task state and progress
	found := false
	for i, task := range f.manager.state.Migrations.Tasks {
		if task.ID == cmd.MigrationTask.ID {
			// Update state only if it's a valid transition
			if isValidStateTransition(task.State, cmd.MigrationTask.State) {
				f.manager.state.Migrations.Tasks[i].State = cmd.MigrationTask.State
			}
			f.manager.state.Migrations.Tasks[i].MovedKeys = cmd.MigrationTask.MovedKeys
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("migration task %s not found", cmd.MigrationTask.ID)
	}

	// Check if all migrations are completed
	allCompleted := true
	for _, task := range f.manager.state.Migrations.Tasks {
		if task.State != MigrationCompleted {
			allCompleted = false
			break
		}
	}

	// Clear migration plan if all tasks are completed
	if allCompleted {
		f.manager.state.Migrations = nil
	}

	return nil
}

// Helper function to validate state transitions
func isValidStateTransition(from, to MigrationState) bool {
	switch from {
	case MigrationPending:
		return to == MigrationInProgress
	case MigrationInProgress:
		return to == MigrationCompleted
	default:
		return false
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.manager.stateLock.RLock()
	defer f.manager.stateLock.RUnlock()

	// Create a deep copy of the state
	nodes := make(map[string]*Node)
	for k, v := range f.manager.state.Nodes {
		nodeCopy := *v
		nodes[k] = &nodeCopy
	}

	var migrations *MigrationPlan
	if f.manager.state.Migrations != nil {
		migrations = &MigrationPlan{
			Tasks: make([]*MigrationTask, len(f.manager.state.Migrations.Tasks)),
		}
		for i, task := range f.manager.state.Migrations.Tasks {
			taskCopy := *task
			migrations.Tasks[i] = &taskCopy
		}
	}

	state := &State{
		Nodes:      nodes,
		Migrations: migrations,
	}

	return &Snapshot{state: state}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	state := &State{
		Nodes: make(map[string]*Node),
	}

	if err := json.NewDecoder(rc).Decode(state); err != nil {
		return err
	}

	f.manager.stateLock.Lock()
	f.manager.state = state
	nodes := f.manager.GetActiveNodes()
	f.manager.ring = f.manager.createNewRing(nodes)
	f.manager.stateLock.Unlock()

	return nil
}

type Snapshot struct {
	state *State
}

// Persist saves the FSM snapshot to the given sink
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode the state
		b, err := json.Marshal(s.state)
		if err != nil {
			return err
		}

		// Write the snapshot to the sink
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is called when the FSM is done with the snapshot
func (s *Snapshot) Release() {
	// No resources to release
}
