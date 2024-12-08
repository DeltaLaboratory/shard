package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buraksezer/consistent"

	"github.com/DeltaLaboratory/dkv/internal/storage"
)

const (
	batchSize = 1000
)

type MigrationManager struct {
	manager *Manager
	store   *storage.PebbleStore
}

func NewMigrationManager(manager *Manager, store *storage.PebbleStore) *MigrationManager {
	return &MigrationManager{
		manager: manager,
		store:   store,
	}
}

// CreateMigrationPlan creates a plan for redistributing data
func (m *Manager) CreateMigrationPlan(oldRing, newRing *consistent.Consistent) (*MigrationPlan, error) {
	plan := &MigrationPlan{
		Tasks: make([]*MigrationTask, 0),
	}

	// Get all keys from storage
	keys, err := m.store.GetAllKeys()
	if err != nil {
		return nil, err
	}

	// Group keys by target node in the new ring
	keysByNewNode := make(map[string][][]byte)
	for _, key := range keys {
		oldNode := oldRing.LocateKey(key)
		newNode := newRing.LocateKey(key)

		if oldNode.String() != newNode.String() {
			keysByNewNode[newNode.String()] = append(keysByNewNode[newNode.String()], key)
		}
	}

	// Create migration tasks
	for nodeID, keys := range keysByNewNode {
		if len(keys) == 0 {
			continue
		}

		task := &MigrationTask{
			ID:         fmt.Sprintf("migration-%s-%d", nodeID, time.Now().UnixNano()),
			SourceNode: m.nodeID,
			TargetNode: nodeID,
			State:      MigrationPending,
			StartKey:   keys[0],
			EndKey:     keys[len(keys)-1],
			TotalKeys:  int64(len(keys)),
			MovedKeys:  0,
		}
		plan.Tasks = append(plan.Tasks, task)
	}

	return plan, nil
}

// ExecuteMigrationTask executes a single migration task
func (mm *MigrationManager) ExecuteMigrationTask(ctx context.Context, task *MigrationTask) error {
	// Update task state to InProgress
	if err := mm.updateTaskState(ctx, task, MigrationInProgress); err != nil {
		return fmt.Errorf("failed to update task state: %v", err)
	}

	// Create iterator for the key range
	iter := mm.store.NewIterator(task.StartKey, task.EndKey)
	defer iter.Close()

	batch := make(map[string][]byte, batchSize)

	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			key := iter.Key()
			value := iter.Value()

			// Check if key belongs to target node in new ring
			targetNode, err := mm.manager.GetNode(key)
			if err != nil {
				return fmt.Errorf("failed to get target node: %v", err)
			}

			if targetNode.ID == task.TargetNode {
				batch[string(key)] = value

				if len(batch) >= batchSize {
					if err := mm.transferBatch(ctx, task.TargetNode, batch); err != nil {
						return fmt.Errorf("failed to transfer batch: %v", err)
					}
					task.MovedKeys += int64(len(batch))

					if err := mm.updateTaskProgress(ctx, task); err != nil {
						return fmt.Errorf("failed to update progress: %v", err)
					}

					batch = make(map[string][]byte, batchSize)
				}
			}
		}
	}

	// Transfer remaining batch
	if len(batch) > 0 {
		if err := mm.transferBatch(ctx, task.TargetNode, batch); err != nil {
			return fmt.Errorf("failed to transfer final batch: %v", err)
		}
		task.MovedKeys += int64(len(batch))

		if err := mm.updateTaskProgress(ctx, task); err != nil {
			return fmt.Errorf("failed to update final progress: %v", err)
		}
	}

	// Update task state to Completed
	return mm.updateTaskState(ctx, task, MigrationCompleted)
}

func (mm *MigrationManager) updateTaskState(ctx context.Context, task *MigrationTask, state MigrationState) error {
	cmd := &Command{
		Op: CommandUpdateMigrationTask,
		MigrationTask: &MigrationTask{
			ID:        task.ID,
			State:     state,
			MovedKeys: task.MovedKeys,
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return mm.manager.Raft.Apply(data, 10*time.Second).Error()
}

func (mm *MigrationManager) updateTaskProgress(ctx context.Context, task *MigrationTask) error {
	cmd := &Command{
		Op: CommandUpdateMigrationTask,
		MigrationTask: &MigrationTask{
			ID:        task.ID,
			State:     MigrationInProgress,
			MovedKeys: task.MovedKeys,
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return mm.manager.Raft.Apply(data, 10*time.Second).Error()
}

func (mm *MigrationManager) transferBatch(ctx context.Context, nodeID string, batch map[string][]byte) error {
	// Get client from pool
	pc, err := mm.manager.clients.GetClient(nodeID, mm.manager.state.Nodes[nodeID].Address)
	if err != nil {
		return fmt.Errorf("failed to get client: %v", err)
	}

	// Transfer data to target node
	if err := pc.BatchSet(ctx, batch); err != nil {
		return fmt.Errorf("failed to set batch on target: %v", err)
	}

	// After successful transfer, delete from source
	wb := mm.store.NewBatch()
	defer wb.Close()

	for key := range batch {
		if err := wb.Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to stage delete for migrated key: %v", err)
		}
	}

	// Commit the batch delete
	if err := wb.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch delete: %v", err)
	}

	return nil
}
