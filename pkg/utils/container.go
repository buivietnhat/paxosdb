package utils

import "container/heap"

// An IntHeap is a min-heap of ints.
type IntHeap []int

func NewIntHeap() *IntHeap {
  h := &IntHeap{}
  heap.Init(h)
  return h
}

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x any) {
  // Push and Pop use pointer receivers because they modify the slice's length,
  // not just its contents.
  *h = append(*h, x.(int))
}

func (h *IntHeap) Pop() any {
  old := *h
  n := len(old)
  x := old[n-1]
  *h = old[0 : n-1]
  return x
}

func (h *IntHeap) HPush(x any) {
  heap.Push(h, x)
}

func (h *IntHeap) HPop() any {
  return heap.Pop(h)
}

func (h *IntHeap) Top() any {
  if h.Len() == 0 {
    return nil
  }

  return (*h)[0]
}
