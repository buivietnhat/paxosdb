package kvpaxos

import "sync"

type Filter[K any, V any] interface {
  Exist(K) (bool, V, error)
  Record(K, V) error
}

type HashFilter[K comparable, V any] struct {
  tb map[K]V
}

func NewHashFilter[K comparable, V any]() *HashFilter[K, V] {
  return &HashFilter[K, V]{
    tb: make(map[K]V),
  }
}

func (h *HashFilter[K, V]) Exist(k K) (bool, V, error) {
  v, existed := h.tb[k]
  return existed, v, nil
}

func (h *HashFilter[K, V]) Record(k K, v V) error {
  h.tb[k] = v
  return nil
}

type Pairer[F any, S any] interface {
  First() F
  Second() S
}

type DoubleHashFilter[F, S comparable, P Pairer[F, S], V any] struct {
  mu sync.Mutex
  tb map[F]*HashFilter[S, V]
}

func NewDoubleHashFilter[F, S comparable, P Pairer[F, S], V any]() *DoubleHashFilter[F, S, P, V] {
  return &DoubleHashFilter[F, S, P, V]{
    tb: make(map[F]*HashFilter[S, V]),
  }
}

func (d *DoubleHashFilter[F, S, P, V]) Exist(p P) (bool, V, error) {
  d.mu.Lock()
  defer d.mu.Unlock()

  if ht, exist := d.tb[p.First()]; exist {
    if ee, vv, errr := ht.Exist(p.Second()); errr == nil && ee {
      return true, vv, nil
    }
  }

  var v V
  return false, v, nil
}

func (d *DoubleHashFilter[F, S, P, V]) Record(p P, v V) error {
  d.mu.Lock()
  defer d.mu.Unlock()

  if _, exist := d.tb[p.First()]; !exist {
    d.tb[p.First()] = NewHashFilter[S, V]()
  }

  d.tb[p.First()].Record(p.Second(), v)
  return nil
}
