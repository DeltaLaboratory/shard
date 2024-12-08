package server

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/lesismal/arpc"
	"github.com/rs/zerolog"

	"github.com/DeltaLaboratory/shard/internal/cluster"
	"github.com/DeltaLaboratory/shard/internal/protocol"
	"github.com/DeltaLaboratory/shard/internal/storage"
)

type Server struct {
	nodeID     string
	raftAddr   string
	serverAddr string
	dataDir    string
	store      *storage.PebbleStore
	cluster    *cluster.Manager
	rpcServer  *arpc.Server
	handler    *Handler

	logger zerolog.Logger
}

func NewServer(nodeID, raftAddr, serverAddr, dataDir string, logger zerolog.Logger) (*Server, error) {
	storePath := filepath.Join(dataDir, "store")
	store, err := storage.NewPebbleStore(storePath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}

	// Initialize cluster manager
	clusterMgr, err := cluster.NewManager(
		nodeID,
		raftAddr,
		serverAddr,
		store,
		filepath.Join(dataDir, "raft"),
		logger,
	)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to create cluster manager: %v", err)
	}

	// Create RPC server
	rpcServer := arpc.NewServer()

	handler, err := NewHandler(store)
	if err != nil {
		if err := store.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close store")
		}
		if err := clusterMgr.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close cluster manager")
		}
		return nil, fmt.Errorf("failed to create handler: %v", err)
	}

	s := &Server{
		nodeID:     nodeID,
		raftAddr:   raftAddr,
		serverAddr: serverAddr,
		dataDir:    dataDir,
		store:      store,
		cluster:    clusterMgr,
		rpcServer:  rpcServer,
		handler:    handler,

		logger: logger,
	}

	// Register RPC methods
	rpcServer.Handler.Handle("/kv/get", s.handleGet)
	rpcServer.Handler.Handle("/kv/set", s.handleSet)
	rpcServer.Handler.Handle("/kv/delete", s.handleDelete)
	rpcServer.Handler.Handle("/kv/redirect", s.handleRedirect)
	rpcServer.Handler.Handle("/cluster/join", s.handleJoin)
	rpcServer.Handler.Handle("/cluster/leave", s.handleLeave)
	rpcServer.Handler.Handle("/cluster/leader", s.handleClusterLeader)
	rpcServer.Handler.Handle("/cluster/info", s.handleClusterInfo)

	return s, nil
}

func (s *Server) Cluster() *cluster.Manager {
	return s.cluster
}

func (s *Server) Start() error {
	// Start RPC server
	if err := s.rpcServer.Run(s.serverAddr); err != nil {
		return fmt.Errorf("failed to start RPC server: %v", err)
	}

	return nil
}

func (s *Server) Bootstrap() error {
	return s.cluster.Bootstrap()
}

func (s *Server) Join(joinAddr string) error {
	// Create temporary client to join cluster
	client, err := arpc.NewClient(func() (net.Conn, error) {
		return net.Dial("tcp", joinAddr)
	})
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %v", err)
	}
	defer client.Stop()

	// Send join request
	req := &protocol.ServerInfo{
		ID:          s.nodeID,
		RaftAddress: s.raftAddr,
		RPCAddress:  s.serverAddr,
	}

	if err := client.Call("/cluster/join", req, &struct{}{}, time.Minute); err != nil {
		return fmt.Errorf("failed to join cluster: %v", err)
	}

	s.logger.Info().Any("nodes", s.cluster.GetActiveNodes(false)).Msg("joined cluster")

	return nil
}

func (s *Server) Leave() error {
	s.logger.Info().Msg("leaving cluster")

	leader, err := s.cluster.GetLeader()
	if err != nil {
		return fmt.Errorf("failed to get leader: %v", err)
	}

	s.logger.Info().Str("leader", leader).Msg("found leader")

	node, err := s.cluster.GetNodeByID(leader)
	if err != nil {
		return fmt.Errorf("failed to get leader node: %v", err)
	}

	// Create temporary client to leave cluster
	client, err := arpc.NewClient(func() (net.Conn, error) {
		return net.Dial("tcp", node.RPCAddress)
	})
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %v", err)
	}
	defer client.Stop()

	// Send leave request
	req := &protocol.ServerInfo{
		ID:          s.nodeID,
		RaftAddress: s.raftAddr,
		RPCAddress:  s.serverAddr,
	}

	if err := client.Call("/cluster/leave", req, &struct{}{}, time.Minute); err != nil {
		return fmt.Errorf("failed to leave cluster: %v", err)
	}

	return nil
}

func (s *Server) Stop() error {
	if err := s.Leave(); err != nil {
		s.logger.Error().Err(err).Msg("failed to leave cluster")
	}

	if err := s.rpcServer.Stop(); err != nil {
		s.logger.Error().Err(err).Msg("failed to stop RPC server")
	}

	if err := s.cluster.Close(); err != nil {
		s.logger.Error().Err(err).Msg("failed to close cluster manager")
	}

	// Close storage
	if err := s.store.Close(); err != nil {
		s.logger.Error().Err(err).Msg("failed to close storage")
	}

	return nil
}

func (s *Server) handleClusterLeader(ctx *arpc.Context) {
	_, id := s.cluster.Raft.LeaderWithID()
	node, err := s.cluster.GetNodeByID(string(id))
	if err != nil {
		if err := ctx.Error(fmt.Errorf("failed to retrieve leader node")); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/leader").Msg("failed to send error response")
		}
		return
	}

	if err := ctx.Write(&protocol.ServerInfo{
		ID:          string(id),
		RaftAddress: node.RaftAddress,
		RPCAddress:  node.RPCAddress,
	}); err != nil {
		s.logger.Error().Err(err).Str("handler", "cluster/leader").Msg("failed to write response")
	}
}

func (s *Server) handleClusterInfo(ctx *arpc.Context) {
	if s.cluster.Raft.State() != raft.Leader {
		leader, _ := s.cluster.GetLeader()
		if err := ctx.Write(&protocol.ClusterInfoResponse{
			Error: fmt.Sprintf("not leader, redirect to %s", leader),
		}); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/info").Msg("failed to write response")
		}
		return
	}

	nodes := s.cluster.GetActiveNodes(false)
	servers := make([]protocol.ServerInfo, len(nodes))

	for i, node := range nodes {
		servers[i] = protocol.ServerInfo{
			ID:          node.ID,
			RaftAddress: node.RaftAddress,
			RPCAddress:  node.RPCAddress,
		}
	}

	if err := ctx.Write(&protocol.ClusterInfoResponse{
		Servers: servers,
	}); err != nil {
		s.logger.Error().Err(err).Str("handler", "cluster/info").Msg("failed to write response")
	}
}

func (s *Server) handleGet(ctx *arpc.Context) {
	var req protocol.KVRequest
	if err := ctx.Bind(&req); err != nil {
		s.logger.Warn().Err(err).Str("handler", "kv/get").Msg("failed to bind request")

		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to send error response")
		}
		return
	}

	if ok, err := s.isResponsibleForKey(req.Key); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to send error response")
		}
		return
	} else if !ok {
		// Redirect
		node, err := s.cluster.GetNode(req.Key)
		if err != nil {
			s.logger.Warn().Err(err).Str("handler", "kv/get").Msg("failed to get node for key")

			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to send error response")
			}
			return
		}

		if err := ctx.Write(&protocol.KVResponse{
			RedirectTo: node.RPCAddress,
		}); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to write response")
		}
		return
	}

	resp, err := s.handler.Get(ctx, &req)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to send error response")
		}
		return
	}

	if err := ctx.Write(resp); err != nil {
		s.logger.Error().Err(err).Str("handler", "kv/get").Msg("failed to write response")
	}
}

func (s *Server) handleSet(ctx *arpc.Context) {
	var req protocol.KVRequest
	if err := ctx.Bind(&req); err != nil {
		s.logger.Warn().Err(err).Str("handler", "kv/set").Str("handler", "set").Msg("failed to bind request")

		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to send error response")
		}
		return
	}

	if ok, err := s.isResponsibleForKey(req.Key); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to send error response")
		}
		return
	} else if !ok {
		// Redirect
		node, err := s.cluster.GetNode(req.Key)
		if err != nil {
			s.logger.Warn().Err(err).Str("handler", "kv/set").Msg("failed to get node for key, is hash ring corrupted?")

			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to send error response")
			}
			return
		}

		if err := ctx.Write(&protocol.KVResponse{
			RedirectTo: node.RPCAddress,
		}); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to write response")
		}
	}

	resp, err := s.handler.Set(ctx, &req)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to send error response")
		}

		return
	}

	if err := ctx.Write(resp); err != nil {
		s.logger.Error().Err(err).Str("handler", "kv/set").Msg("failed to write response")
	}
}

func (s *Server) handleDelete(ctx *arpc.Context) {
	var req protocol.KVRequest
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to send error response")
		}
		return
	}

	if ok, err := s.isResponsibleForKey(req.Key); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to send error response")
		}
		return
	} else if !ok {
		// Redirect
		node, err := s.cluster.GetNode(req.Key)
		if err != nil {
			s.logger.Warn().Err(err).Str("handler", "kv/delete").Msg("failed to get node, is hash ring corrupted?")

			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to send error response")
			}
			return
		}

		if err := ctx.Write(&protocol.KVResponse{
			RedirectTo: node.RPCAddress,
		}); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to write response")
		}
	}

	resp, err := s.handler.Delete(ctx, &req)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to send error response")
		}
		return
	}

	if err := ctx.Write(resp); err != nil {
		s.logger.Error().Err(err).Str("handler", "kv/delete").Msg("failed to write response")
	}
}

func (s *Server) handleRedirect(ctx *arpc.Context) {
	var req protocol.KVRequest
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
		}
		return
	}

	// Double-check if we should handle this key
	responsible, err := s.isResponsibleForKey(req.Key)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
		}
		return
	}

	if !responsible {
		if err := ctx.Error(fmt.Errorf("server not responsible for key")); err != nil {
			s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
		}
		return
	}

	// Handle the redirected request based on operation type
	switch req.Op {
	case protocol.OpGet:
		if resp, err := s.handler.Get(ctx, &req); err != nil {
			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
			}
		} else {
			if err := ctx.Write(resp); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to write response")
			}
		}

	case protocol.OpSet:
		if resp, err := s.handler.Set(ctx, &req); err != nil {
			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
			}
		} else {
			if err := ctx.Write(resp); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to write response")
			}
		}

	case protocol.OpDelete:
		if resp, err := s.handler.Delete(ctx, &req); err != nil {
			if err := ctx.Error(err); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
			}
		} else {
			if err := ctx.Write(resp); err != nil {
				s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to write response")
			}
		}

	default:
		if err := ctx.Error(fmt.Errorf("unknown operation type")); err != nil {
			s.logger.Error().Err(err).Str("handler", "redirect").Msg("failed to send error response")
		}
	}
}

func (s *Server) handleJoin(ctx *arpc.Context) {
	var req protocol.ServerInfo
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/join").Msg("failed to send error response")
		}
		return
	}

	err := s.cluster.Join(req.ID, req.RaftAddress, req.RPCAddress)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/join").Msg("failed to send error response")
		}
		return
	}

	if err := ctx.Write(&struct{}{}); err != nil {
		s.logger.Error().Err(err).Str("handler", "cluster/join").Msg("failed to write response")
	}
}

func (s *Server) handleLeave(ctx *arpc.Context) {
	var req protocol.ServerInfo
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/leave").Msg("failed to send error response")
		}
		return
	}

	err := s.cluster.Leave(req.ID)
	if err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/leave").Msg("failed to send error response")
		}
		return
	}

	if err := ctx.Write(&struct{}{}); err != nil {
		s.logger.Error().Err(err).Str("handler", "cluster/leave").Msg("failed to write response")
	}
}

func (s *Server) isResponsibleForKey(key []byte) (bool, error) {
	// Get the node that should handle this key
	node, err := s.cluster.GetNode(key)
	if err != nil {
		return false, err
	}

	// Check if it's us
	return node.ID == s.cluster.NodeID(), nil
}
