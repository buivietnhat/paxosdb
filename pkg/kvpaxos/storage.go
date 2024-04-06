package kvpaxos

import (
  "errors"
)

type Getter[K any, V any] interface {
  Get(K) (V, error)
}

type Putter[K any, V any] interface {
  Put(K, V) error
}

type Appender[K any, V any] interface {
  Append(K, V) error
}

type Deleter[K any] interface {
  Delete(K) error
}

type Storer[K any, V any] interface {
  Getter[K, V]
  Putter[K, V]
  Appender[K, V]
  Deleter[K]
}

type HashDB struct {
  //mu sync.Mutex
  db map[string]string
}

func (h HashDB) Get(key string) (string, error) {
  //h.mu.Lock()
  //defer h.mu.Unlock()
  v, ok := h.db[key]
  if !ok {
    return "", errors.New("key not existed")
  }
  return v, nil
}

func (h HashDB) Put(key string, value string) error {
  //h.mu.Lock()
  //defer h.mu.Unlock()

  h.db[key] = value
  return nil
}

func (h HashDB) Append(key string, value string) error {
  //h.mu.Lock()
  //defer h.mu.Unlock()

  v := h.db[key]
  h.db[key] = v + value
  return nil
}

func (h HashDB) Delete(key string) error {
  //h.mu.Lock()
  //defer h.mu.Unlock()

  delete(h.db, key)
  return nil
}

func NewHashDb() *HashDB {
  return &HashDB{db: make(map[string]string)}
}
