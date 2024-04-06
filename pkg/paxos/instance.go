package paxos

import (
	"fmt"
	. "pxdb/pkg/logger"
	"sync"
	"sync/atomic"
)

type Instance struct {
  Proposer
  Acceptor

  mu   sync.Mutex
  done chan struct{}
  me   int
  seq  int

  doneSeq int // current seq # that the Paxo

  peers  []string
  logger Logger
  dead   int32

  updateVal func(int, interface{})
}

func (ins *Instance) UpdateDoneSeq(seq int) {
  ins.mu.Lock()
  defer ins.mu.Unlock()
  ins.doneSeq = seq
}

func NewInstance(me int, seq int, peers []string, done int, logger Logger, updateFunc func(int, interface{})) *Instance {
  ins := &Instance{me: me, seq: seq, peers: peers, logger: logger, doneSeq: done, updateVal: updateFunc, done: make(chan struct{})}
  ins.Proposer = NewProposer(ins)
  ins.Acceptor = NewAcceptor(ins)
  return ins
}

func (ins *Instance) Start(v interface{}) {
  for !ins.killed() {
    n, va := ins.prepare()
    if va != nil && va != v {
      ins.log(DWarn, "value %v already decided, just commit with that value ...", va)
      v = va
    }

    ok := ins.acceptAll(n, v)
    if ok {
      ins.decideAll(n, v)
      ins.updateVal(ins.seq, v)
      break
    } else {
      ins.log(DInfo, "Coundn't finish aggrement with n %d, trying a new one", n)
    }
  }
}

func (ins *Instance) Kill() {
  atomic.StoreInt32(&ins.dead, 1)
  close(ins.done)
}

func (ins *Instance) killed() bool {
  d := atomic.LoadInt32(&ins.dead)
  return d == 1
}

func (ins *Instance) log(topic LogTopic, format string, a ...interface{}) {
  if DebugEnabled() {
    msg := fmt.Sprintf(format, a...)
    ins.logger.Debug(topic, "{%d} %v", ins.seq, msg)
  }
}
