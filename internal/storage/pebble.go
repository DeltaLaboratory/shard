package storage

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog"
)

type PebbleStore struct {
	db *pebble.DB

	logger zerolog.Logger
}

func NewPebbleStore(path string, logger zerolog.Logger) (*PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{
		db:     db,
		logger: logger.With().Str("layer", "storage").Logger(),
	}, nil
}

func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer func() {
		if err := closer.Close(); err != nil {
			s.logger.Warn().Err(err).Msg("failed to close pebble iterator")
		}
	}()

	// Copy the value since it's only valid while closer is not closed
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (s *PebbleStore) Set(key, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

func (s *PebbleStore) Delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

type Iterator struct {
	iter *pebble.Iterator
}

func (s *PebbleStore) NewIterator(start, end []byte) *Iterator {
	opts := &pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	// TODO: Handle error
	iter, err := s.db.NewIter(opts)
	if err != nil {
		return nil
	}
	return &Iterator{iter: iter}
}

func (i *Iterator) Valid() bool {
	return i.iter.Valid()
}

func (i *Iterator) First() bool {
	return i.iter.First()
}

func (i *Iterator) Next() bool {
	return i.iter.Next()
}

func (i *Iterator) Key() []byte {
	return i.iter.Key()
}

func (i *Iterator) Value() []byte {
	val := i.iter.Value()
	result := make([]byte, len(val))
	copy(result, val)
	return result
}

func (i *Iterator) Close() error {
	return i.iter.Close()
}

func (s *PebbleStore) GetAllKeys() ([][]byte, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		keys = append(keys, key)
	}
	return keys, nil
}

type Batch struct {
	batch *pebble.Batch
	db    *pebble.DB
}

func (s *PebbleStore) NewBatch() *Batch {
	return &Batch{
		batch: s.db.NewBatch(),
		db:    s.db,
	}
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value, pebble.Sync)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key, pebble.Sync)
}

func (b *Batch) Commit() error {
	return b.batch.Commit(pebble.Sync)
}

func (b *Batch) Close() error {
	return b.batch.Close()
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}
