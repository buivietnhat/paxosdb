package paxos

import (
	"errors"
	"math/rand"
	. "pxdb/pkg/logger"
	"sync"
	"time"
)

type PrepareResult struct {
  n  Np
  v  interface{}
  ok bool
}

type AcceptResult struct {
  Ok bool
}

type DecideResult struct {
  Ok bool
}

type Proposer struct {
  n     Np
  tentn Np
  ins   *Instance
  mu    sync.Mutex
}

func NewProposer(ins *Instance) Proposer {
  return Proposer{ins: ins}
}

func (p *Proposer) newRound() Np {
  p.mu.Lock()
  defer p.mu.Unlock()

  p.tentn = p.tentn.Increase(p.ins.me)
  p.n = p.tentn
  return p.n
}

func (p *Proposer) updateTenN(n Np) {
  if p.tentn.LessThan(n) {
    p.tentn = n
  }
}

func (p *Proposer) UpdateTentN(n Np) {
  p.mu.Lock()
  defer p.mu.Unlock()
  p.updateTenN(n)
}

func (p *Proposer) prepare() (Np, interface{}) {
  n := p.newRound()
  va, err := p.prepareAll(n)

  for err != nil && !p.ins.killed() {
    // small random sleep to prevent multiple proposals keep interfering each other
    ms := (rand.Int63() % 100)
    time.Sleep(time.Duration(ms) * time.Millisecond)

    n = p.newRound()
    p.ins.log(DProposer, "Old proposal is not accepted, try with new one %d", n.N)

    va, err = p.prepareAll(n)
  }

  return n, va
}

func (p *Proposer) prepareAll(n Np) (interface{}, error) {
  //p.ins.log(DProposer, "Sending Prepare requests with value %d", n)

  ch := make(chan PrepareResult)
  finish := make(chan struct{})

  for s, _ := range p.ins.peers {
    go p.prepareOne(n, s, ch, finish)
  }

  var (
    maxna    = NewNp(p.ins.me)
    va       interface{}
    received = 0
    okCount  = 0
  )

  for {
    select {
    case r := <-ch:
      received += 1
      if r.ok {
        okCount += 1
        if maxna.LessThan(r.n) {
          maxna = r.n
          va = r.v
        }
      }

      if okCount > len(p.ins.peers)/2 || received == len(p.ins.peers) {
        close(finish)
        goto FINISH
      }
    case <-p.ins.done:
      goto FINISH
    }
  }

FINISH:
  if okCount > len(p.ins.peers)/2 {
    return va, nil
  }

  return va, errors.New("proposal is not accepted")

}

func (p *Proposer) prepareOne(n Np, s int, ch chan PrepareResult, finish chan struct{}) {
  args := PrepareArgs{S: p.ins.me, N: n, Done: p.ins.doneSeq, Seq: p.ins.seq}
  reply := PrepareReply{}

  p.ins.log(DProposer, "Sending Prepare for server %d with n %d", s, n)

  if s != p.ins.me {
    call(p.ins.peers[s], "Paxos.Prepare", &args, &reply)
  } else {
    p.ins.Prepare(&args, &reply)
  }

  pr := PrepareResult{n: reply.Na, v: reply.Va}

  if reply.Err == Ok {
    pr.ok = true
    p.ins.log(DProposer, "Sent Prepare for server %d OK with rep n %d v %v", s, reply.Na, reply.Va)
  } else {
    pr.ok = false
    p.ins.log(DDrop, "Sent Prepare for server %d num %d failed with Err %v", s, n, String(reply.Err))
    p.UpdateTentN(reply.Np)
  }

  select {
  case ch <- pr:
  case <-p.ins.done:
  case <-finish:
  }

}

func (p *Proposer) acceptAll(n Np, v interface{}) bool {
  var (
    received = 0
    okCount  = 0
    ch       = make(chan AcceptResult)
    finish   = make(chan struct{})
  )

  for s, _ := range p.ins.peers {
    go p.acceptOne(s, n, v, ch, finish)
  }

  for {
    select {
    case r := <-ch:
      received += 1
      if r.Ok {
        okCount += 1
      }

      if okCount > len(p.ins.peers)/2 || received == len(p.ins.peers) {
        goto FINISH
      }
    case <-p.ins.done:
      goto FINISH
    }
  }

FINISH:
  if okCount > len(p.ins.peers)/2 {
    p.ins.log(DProposer, "Send Accept with n %d  Ok for majority", n)
    return true
  }

  p.ins.log(DDrop, "Send Accept with n %d  Failed for majority", n)
  return false
}

func (p *Proposer) acceptOne(s int, n Np, v interface{}, ch chan AcceptResult, finish chan struct{}) {
  args := AcceptArgs{S: p.ins.me, N: n, V: v, Done: p.ins.doneSeq, Seq: p.ins.seq}
  reply := AcceptReply{}

  p.ins.log(DProposer, "Sending Accpept for server %d with n %d ", s, n)

  if s != p.ins.me {
    call(p.ins.peers[s], "Paxos.Accept", &args, &reply)
  } else {
    p.ins.Accept(&args, &reply)
  }

  ar := AcceptResult{}

  if reply.Err == Ok {
    p.ins.log(DProposer, "Sending Accpept for server %d with n %d OK", s, n)
    ar.Ok = true
  } else {
    p.ins.log(DDrop, "Sending Accpept for server %d with n %d Failed", s, n)
    ar.Ok = false
    p.UpdateTentN(reply.Np)
  }

  select {
  case ch <- ar:
  case <-p.ins.done:
  case <-finish:
  }
}

func (p *Proposer) decideAll(n Np, v interface{}) {
  for s, _ := range p.ins.peers {
    go p.decideOne(s, n, v)
  }
}

func (p *Proposer) decideOne(s int, n Np, v interface{}) {
  args := DecideArgs{V: v, Done: p.ins.doneSeq, Seq: p.ins.seq, S: p.ins.me}
  reply := DecideReply{}

  p.ins.log(DProposer, "Sending Decide for server %d with n %d ", s, n)
  if s != p.ins.me {
    call(p.ins.peers[s], "Paxos.Decide", &args, &reply)
  } else {
    p.ins.Decide(&args, &reply)
  }

  if reply.Err == Ok {
    p.ins.log(DProposer, "Sending Decide for server %d with n %d OK", s, n)
  } else {
    p.ins.log(DWarn, "Sending Decide for server %d with n %d Failed", s, n)
    if !p.ins.killed() {
      time.Sleep(1 * time.Second)
      p.ins.log(DProposer, "Retry sending decide for server %d with n %d", s, n)
      p.decideOne(s, n, v)
    }
  }
}
