package cluster

type NodeState uint8

const (
	NodeStateActive NodeState = iota
	NodeStateLeaving
	NodeStateJoining
)

type MigrationState uint8

const (
	MigrationPending MigrationState = iota
	MigrationInProgress
	MigrationCompleted
)

type Node struct {
	ID          string    `json:"id"`
	RaftAddress string    `json:"raft_address"`
	RPCAddress  string    `json:"rpc_address"`
	State       NodeState `json:"state"`
	HashKey     uint64    `json:"hash_key"`
}

func (n *Node) String() string {
	return n.ID
}

type State struct {
	Nodes      map[string]*Node `json:"nodes"`
	Migrations *MigrationPlan   `json:"migrations,omitempty"`
}

type MigrationTask struct {
	ID         string         `json:"id"`
	SourceNode string         `json:"source_node"`
	TargetNode string         `json:"target_node"`
	State      MigrationState `json:"state"`
	StartKey   []byte         `json:"start_key"`
	EndKey     []byte         `json:"end_key"`
	TotalKeys  int64          `json:"total_keys"`
	MovedKeys  int64          `json:"moved_keys"`
}

type MigrationPlan struct {
	Tasks []*MigrationTask `json:"tasks"`
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
