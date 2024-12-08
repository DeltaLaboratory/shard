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
	f.manager.logger.Debug().
		Uint64("index", log.Index).
		Uint64("term", log.Term).
		Msg("Starting Apply method")

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.manager.logger.Error().Err(err).
			Str("log_data", string(log.Data)).
			Msg("Failed to unmarshal command")
		return err
	}

	f.manager.logger.Debug().
		Interface("command_type", cmd.Op).
		Interface("node", cmd.Node).
		Interface("migration_plan", cmd.MigrationPlan).
		Interface("migration_task", cmd.MigrationTask).
		Msg("Processing command")

	f.manager.stateLock.Lock()
	defer f.manager.stateLock.Unlock()

	switch cmd.Op {
	case CommandJoinNode:
		f.manager.logger.Debug().Msg("Executing JoinNode command")
		result := f.applyJoinNode(cmd)
		f.manager.logger.Debug().Interface("result", result).Msg("JoinNode command completed")
		return result
	case CommandLeaveNode:
		f.manager.logger.Debug().Msg("Executing LeaveNode command")
		result := f.applyLeaveNode(cmd)
		f.manager.logger.Debug().Interface("result", result).Msg("LeaveNode command completed")
		return result
	case CommandUpdateNodeState:
		f.manager.logger.Debug().Msg("Executing UpdateNodeState command")
		result := f.applyUpdateNodeState(cmd)
		f.manager.logger.Debug().Interface("result", result).Msg("UpdateNodeState command completed")
		return result
	case CommandRemoveNode:
		f.manager.logger.Debug().Msg("Executing RemoveNode command")
		result := f.applyRemoveNode(cmd)
		f.manager.logger.Debug().Interface("result", result).Msg("RemoveNode command completed")
		return result
	case CommandUpdateMigrationTask:
		f.manager.logger.Debug().Msg("Executing UpdateMigrationTask command")
		result := f.applyUpdateMigrationTask(cmd)
		f.manager.logger.Debug().Interface("result", result).Msg("UpdateMigrationTask command completed")
		return result
	default:
		f.manager.logger.Error().Interface("command_type", cmd.Op).Msg("Unknown command type")
		return fmt.Errorf("unknown command type: %v", cmd.Op)
	}
}

func (f *FSM) applyJoinNode(cmd Command) interface{} {
	f.manager.logger.Debug().Msg("Starting applyJoinNode")

	if cmd.Node == nil {
		f.manager.logger.Error().Msg("Node data is missing in JoinNode command")
		return fmt.Errorf("node data is missing")
	}

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Interface("node_state", cmd.Node.State).
		Msg("Adding node to cluster state")

	prevNode, exists := f.manager.state.Nodes[cmd.Node.ID]
	if exists {
		f.manager.logger.Debug().
			Interface("existing_node", prevNode).
			Msg("Node already exists in cluster")
	}

	f.manager.state.Nodes[cmd.Node.ID] = cmd.Node

	if cmd.MigrationPlan != nil {
		f.manager.logger.Debug().
			Interface("migration_plan", cmd.MigrationPlan).
			Msg("Updating migration plan")
		f.manager.state.Migrations = cmd.MigrationPlan
	}

	if cmd.Node.State == NodeStateActive {
		f.manager.logger.Debug().
			Interface("current_state", f.manager.state).
			Msg("Updating ring for active node")

		nodes := f.manager.GetActiveNodes(true)
		f.manager.logger.Debug().
			Interface("active_nodes", nodes).
			Msg("Retrieved active nodes")

		f.manager.ring = f.manager.createNewRing(nodes)
	}

	f.manager.logger.Debug().
		Interface("final_state", f.manager.state).
		Msg("JoinNode command completed")

	return nil
}

func (f *FSM) applyLeaveNode(cmd Command) interface{} {
	f.manager.logger.Debug().Msg("Starting applyLeaveNode")

	if cmd.Node == nil {
		f.manager.logger.Error().Msg("Node data is missing in LeaveNode command")
		return fmt.Errorf("node data is missing")
	}

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Msg("Processing leave node request")

	node, exists := f.manager.state.Nodes[cmd.Node.ID]
	if !exists {
		f.manager.logger.Error().
			Str("node_id", cmd.Node.ID).
			Msg("Node not found in cluster")
		return fmt.Errorf("node %s not found", cmd.Node.ID)
	}

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Interface("old_state", node.State).
		Msg("Updating node state to leaving")

	node.State = NodeStateLeaving

	if cmd.MigrationPlan != nil {
		f.manager.logger.Debug().
			Interface("migration_plan", cmd.MigrationPlan).
			Msg("Updating migration plan for leaving node")
		f.manager.state.Migrations = cmd.MigrationPlan
	}

	nodes := f.manager.GetActiveNodes(true)
	f.manager.logger.Debug().
		Interface("active_nodes", nodes).
		Msg("Updating ring excluding leaving node")

	f.manager.ring = f.manager.createNewRing(nodes)

	return nil
}

func (f *FSM) applyUpdateNodeState(cmd Command) interface{} {
	f.manager.logger.Debug().Msg("Starting applyUpdateNodeState")

	if cmd.Node == nil {
		f.manager.logger.Error().Msg("Node data is missing in UpdateNodeState command")
		return fmt.Errorf("node data is missing")
	}

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Interface("new_state", cmd.Node.State).
		Msg("Processing node state update")

	node, exists := f.manager.state.Nodes[cmd.Node.ID]
	if !exists {
		f.manager.logger.Error().
			Str("node_id", cmd.Node.ID).
			Msg("Node not found in cluster")
		return fmt.Errorf("node %s not found", cmd.Node.ID)
	}

	oldState := node.State
	node.State = cmd.Node.State

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Interface("old_state", oldState).
		Interface("new_state", node.State).
		Msg("Node state updated")

	if (oldState != NodeStateActive && cmd.Node.State == NodeStateActive) ||
		(oldState == NodeStateActive && cmd.Node.State != NodeStateActive) {
		nodes := f.manager.GetActiveNodes(true)
		f.manager.logger.Debug().
			Interface("active_nodes", nodes).
			Msg("Updating ring due to active state change")
		f.manager.ring = f.manager.createNewRing(nodes)
	}

	return nil
}

func (f *FSM) applyRemoveNode(cmd Command) interface{} {
	f.manager.logger.Debug().Msg("Starting applyRemoveNode")

	if cmd.Node == nil {
		f.manager.logger.Error().Msg("Node data is missing in RemoveNode command")
		return fmt.Errorf("node data is missing")
	}

	f.manager.logger.Debug().
		Str("node_id", cmd.Node.ID).
		Msg("Removing node from cluster")

	if _, exists := f.manager.state.Nodes[cmd.Node.ID]; !exists {
		f.manager.logger.Warn().
			Str("node_id", cmd.Node.ID).
			Msg("Attempting to remove non-existent node")
	}

	delete(f.manager.state.Nodes, cmd.Node.ID)

	nodes := f.manager.GetActiveNodes(true)
	f.manager.logger.Debug().
		Interface("active_nodes", nodes).
		Msg("Updating ring after node removal")

	f.manager.ring = f.manager.createNewRing(nodes)

	return nil
}

func (f *FSM) applyUpdateMigrationTask(cmd Command) interface{} {
	if cmd.MigrationTask == nil {
		return fmt.Errorf("migration task data is missing")
	}

	if f.manager.state.Migrations == nil {
		return fmt.Errorf("no active migration plan")
	}

	f.manager.logger.Debug().Interface("command", cmd).Msg("applying command")

	// Update migration task state and progress
	found := false
	for i, task := range f.manager.state.Migrations.Tasks {
		if task.ID == cmd.MigrationTask.ID {
			f.manager.logger.Debug().Interface("task", task).Interface("updated_task", cmd.MigrationTask).Msg("updating migration task")

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
	nodes := f.manager.GetActiveNodes(true)
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
