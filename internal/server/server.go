package server

import (
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/lesismal/arpc"
	"github.com/rs/zerolog"

	"github.com/DeltaLaboratory/dkv/internal/cluster"
	"github.com/DeltaLaboratory/dkv/internal/protocol"
	"github.com/DeltaLaboratory/dkv/internal/storage"
)

type Server struct {
	handler *Handler
	server  *arpc.Server
	cluster *cluster.Manager

	logger zerolog.Logger
}

func NewServer(nodeID, address, dataPath string, logger zerolog.Logger) (*Server, error) {
	store, err := storage.NewPebbleStore(filepath.Join(dataPath, "store"), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	handler, err := NewHandler(store)
	if err != nil {
		return nil, err
	}

	server := arpc.NewServer()

	clusterManager, err := cluster.NewManager(nodeID, address, store, filepath.Join(dataPath, "raft"))
	if err != nil {
		return nil, err
	}

	s := &Server{
		handler: handler,
		server:  server,
		logger:  logger,
		cluster: clusterManager,
	}

	// Register RPC methods
	server.Handler.Handle("/kv/get", s.handleGet)
	server.Handler.Handle("/kv/set", s.handleSet)
	server.Handler.Handle("/kv/delete", s.handleDelete)
	server.Handler.Handle("/kv/redirect", s.handleRedirect)
	server.Handler.Handle("/cluster/join", s.handleJoin)
	server.Handler.Handle("/cluster/leave", s.handleLeave)
	server.Handler.Handle("/cluster/leader", s.handleClusterLeader)
	server.Handler.Handle("/cluster/info", s.handleClusterInfo)

	return s, nil
}

func (s *Server) Start(addr string) error {
	return s.server.Run(addr)
}

func (s *Server) handleClusterLeader(ctx *arpc.Context) {
	addr, id := s.cluster.Raft.LeaderWithID()
	if err := ctx.Write(&protocol.ServerInfo{
		ID:      string(id),
		Address: string(addr),
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

	nodes := s.cluster.GetActiveNodes()
	servers := make([]protocol.ServerInfo, len(nodes))

	for i, node := range nodes {
		servers[i] = protocol.ServerInfo{
			ID:      node.ID,
			Address: node.Address,
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
			RedirectTo: node.Address,
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
			RedirectTo: node.Address,
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
			RedirectTo: node.Address,
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
	var req struct {
		NodeID  string
		Address string
	}
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/join").Msg("failed to send error response")
		}
		return
	}

	err := s.cluster.Join(req.NodeID, req.Address)
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
	var req struct {
		NodeID string
	}
	if err := ctx.Bind(&req); err != nil {
		if err := ctx.Error(err); err != nil {
			s.logger.Error().Err(err).Str("handler", "cluster/leave").Msg("failed to send error response")
		}
		return
	}

	err := s.cluster.Leave(req.NodeID)
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
