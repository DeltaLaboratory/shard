package protocol

type OperationType uint8

const (
	OpGet OperationType = iota
	OpSet
	OpDelete
)

type KVRequest struct {
	Op    OperationType
	Key   []byte
	Value []byte
}

type KVResponse struct {
	Value      []byte
	Error      string
	RedirectTo string `json:"redirect_to,omitempty"`
}

type ServerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type ClusterInfoResponse struct {
	Servers []ServerInfo `json:"servers"`
	Error   string       `json:"error,omitempty"`
}

type BatchKVRequest struct {
	Batch map[string][]byte `json:"batch"`
}
