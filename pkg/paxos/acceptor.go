package paxos

import (
	. "pxdb/pkg/logger"
	"sync"
)

type Acceptor struct {
  np  Np          // highest prepare seen
  na  Np          // highest accept seen
  va  interface{} // corresponding value
  ins *Instance
  mu  sync.Mutex
}

func NewAcceptor(ins *Instance) Acceptor {
  return Acceptor{ins: ins}
}

func (a *Acceptor) Prepare(args *PrepareArgs, reply *PrepareReply) {
  a.ins.log(DInfo, "Receive Prepare request from server %d with num %d", args.S, args.N)

  a.mu.Lock()
  defer a.mu.Unlock()

  // update my state anyway for the proposal to keep up
  reply.Na = a.na
  reply.Va = a.va
  reply.Np = a.np

  // If I haven't promised to any higher proprosal
  if args.N.GreaterThan(a.np) {
    a.ins.log(DAcceptor, "Accept Prepare request from server %d with num %v, my np %v", args.S, args.N, a.np)
    a.np = args.N
    reply.Err = Ok
  } else {
    a.ins.log(DDrop, "Drop Prepare request from server %d with num %d since I have promised to higher proposal %d",
      args.N.S, args.N, a.np)
    reply.Err = Rejected
  }
}

func (a *Acceptor) Accept(args *AcceptArgs, reply *AcceptReply) {
  a.ins.log(DInfo, "Receive Accept request from server %d with num %d", args.S, args.N)

  a.mu.Lock()
  defer a.mu.Unlock()

  if args.N.GreaterThanOrEqual(a.np) {
    a.np = args.N
    a.na = args.N
    a.va = args.V
    reply.Err = Ok
    a.ins.log(DAcceptor, "Accept request from server %d with num %d my na %d va %v ", args.S, args.N, a.na, a.va)
  } else {
    a.ins.log(DDrop, "Rejected request from server %d with num %d  since my current np is %d",
      args.N.S, args.N, a.np)
    reply.Err = Rejected
  }

  reply.Na = a.na
  reply.Np = a.np
}

func (a *Acceptor) Decide(args *DecideArgs, reply *DecideReply) interface{} {
  a.ins.log(DInfo, "Recide Decide request from Server %d, v %v", args.S, args.V)

  a.mu.Lock()
  defer a.mu.Unlock()

  if a.va != args.V {
    a.va = args.V
  }
  reply.Err = Ok

  return a.va
}
