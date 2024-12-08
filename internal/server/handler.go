package server

import (
	"context"

	"github.com/DeltaLaboratory/dkv/internal/protocol"
	"github.com/DeltaLaboratory/dkv/internal/storage"
)

type Handler struct {
	store *storage.PebbleStore
}

func NewHandler(store *storage.PebbleStore) (*Handler, error) {
	return &Handler{store: store}, nil
}

func (h *Handler) Get(ctx context.Context, req *protocol.KVRequest) (*protocol.KVResponse, error) {
	value, err := h.store.Get(req.Key)
	if err != nil {
		return &protocol.KVResponse{Error: err.Error()}, nil
	}
	return &protocol.KVResponse{Value: value}, nil
}

func (h *Handler) Set(ctx context.Context, req *protocol.KVRequest) (*protocol.KVResponse, error) {
	err := h.store.Set(req.Key, req.Value)
	if err != nil {
		return &protocol.KVResponse{Error: err.Error()}, nil
	}
	return &protocol.KVResponse{}, nil
}

func (h *Handler) Delete(ctx context.Context, req *protocol.KVRequest) (*protocol.KVResponse, error) {
	err := h.store.Delete(req.Key)
	if err != nil {
		return &protocol.KVResponse{Error: err.Error()}, nil
	}
	return &protocol.KVResponse{}, nil
}

func (h *Handler) BatchSet(ctx context.Context, batch map[string][]byte) error {
	// Create a new write batch
	wb := h.store.NewBatch()
	defer wb.Close()

	// Add all key-value pairs to the batch
	for key, value := range batch {
		if err := wb.Set([]byte(key), value); err != nil {
			return err
		}
	}

	// Commit the batch
	return wb.Commit()
}

func (h *Handler) GetKeyRange(ctx context.Context, start, end []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)

	iter := h.store.NewIterator(start, end)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		result[string(key)] = value
	}

	return result, nil
}
