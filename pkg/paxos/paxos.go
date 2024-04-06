package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//

import (
  "fmt"
  "log"
  "math/rand"
  "net"
  "net/rpc"
  "os"
  "pxdb/pkg/utils"
  "sync"
  "sync/atomic"
  "syscall"
  "time"

  . "pxdb/pkg/logger"
)

type Fate int

const (
  Decided   Fate = iota + 1
  Pending        // not yet decided.
  Forgotten      // decided but forgotten.
)

type Np struct {
  N int32 // proposal number
  S int   // server, enforce unique
}

func NewNp(s int) Np {
  return Np{S: s, N: 0}
}

func (n Np) Increase(s int) Np {
  n.S = s
  n.N += 1
  return n
}

func (n Np) LessThan(o Np) bool {
  return n.N < o.N || (n.N == o.N && n.S < o.S)
}

func (n Np) LessThanOrEqual(o Np) bool {
  return !n.GreaterThan(o)
}

func (n Np) GreaterThan(o Np) bool {
  return n.N > o.N || (n.N == o.N && n.S > o.S)
}

func (n Np) GreaterThanOrEqual(o Np) bool {
  return !n.LessThan(o)
}

type Paxos struct {
  mu         sync.Mutex
  l          net.Listener
  dead       int32 // for testing
  unreliable int32 // for testing
  rpcCount   int32 // for testing
  peers      []string
  me         int // index into peers[]

  logger Logger

  // for termination signal
  done chan struct{}

  seqs   *utils.IntHeap
  values map[int]interface{}
  insts  map[int]*Instance

  minDoneSeq map[int]int // each for a remote peer
  maxSeq     int         // max seq that I have seen so far
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      //fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  ins := px.updateInfoFromPeer(args.S, args.Seq, args.Done)

  ins.Prepare(args, reply)
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  ins := px.updateInfoFromPeer(args.S, args.Seq, args.Done)
  ins.Accept(args, reply)
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  ins := px.updateInfoFromPeer(args.S, args.Seq, args.Done)
  v := ins.Decide(args, reply)
  px.updateValueResult(args.Seq, v)
  return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) bool {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  if _, cmited := px.values[seq]; cmited {
    px.logger.Debug(DWarn, "Instance %d is already have a value commited !!!", seq)
    return false
  }

  if _, existed := px.insts[seq]; !existed {
    px.seqs.HPush(seq)
    px.insts[seq] = NewInstance(px.me, seq, px.peers, px.minDoneSeq[px.me], px.logger, px.updateValueResult)
  }

  px.logger.Debug(DClient, "Start new cmd with seq %d", seq)
  go px.insts[seq].Start(v)
  return true
}

func (px *Paxos) updateValueResult(seq int, v interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if _, exist := px.values[seq]; exist {
    //if vv != v {
    //  px.logger.Debug(DError, "commit two different value for same instance %d, %v - %v", seq, vv, v)
    //  panic("commit two different value for same instance !!!")
    //}
    return
  }

  px.logger.Debug(DCommit, "New seq %d commited", seq)
  px.values[seq] = v
  if px.maxSeq < seq {
    px.maxSeq = seq
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  //px.logger.Debug(DTrck, "Client call Done with Seq %d", seq)

  if px.minDoneSeq[px.me] < seq {
    px.minDoneSeq[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  return px.maxSeq
}

func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  return px.min()
}

func (px *Paxos) min() int {
  gmin := px.minDoneSeq[px.me]
  for _, m := range px.minDoneSeq {
    if m < gmin {
      gmin = m
    }
  }

  //px.logger.Debug(DTrck, "Min = %d", min+1)
  return gmin + 1
}

func (px *Paxos) Status(seq int) (Fate, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  v, existed := px.values[seq]

  if !existed {
    return Pending, nil
  }

  if seq < px.min() {
    return Forgotten, nil
  }

  if v != nil {
    return Decided, v
  }

  return Pending, nil
}

func (px *Paxos) Kill() {
  //px.logger.Debug(DTrace, "Got killed ...")
  px.mu.Lock()
  for _, ins := range px.insts {
    ins.Kill()
  }
  px.mu.Unlock()

  close(px.done)

  atomic.StoreInt32(&px.dead, 1)
  if px.l != nil {
    px.l.Close()
  }
}

func (px *Paxos) killed() bool {
  d := atomic.LoadInt32(&px.dead)
  return d == 1
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
  return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
  if what {
    atomic.StoreInt32(&px.unreliable, 1)
  } else {
    atomic.StoreInt32(&px.unreliable, 0)
  }
}

func (px *Paxos) isunreliable() bool {
  return atomic.LoadInt32(&px.unreliable) != 0
}

func Make(peers []string, me int, rpcs *rpc.Server, l Logger) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.logger = l
  px.seqs = utils.NewIntHeap()
  px.insts = make(map[int]*Instance)
  px.values = make(map[int]interface{})
  px.done = make(chan struct{})
  px.minDoneSeq = make(map[int]int)

  px.initMinDoneSeq()
  go px.cleanup()

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me])
    if e != nil {
      log.Fatal("listen error: ", e)
    }
    px.l = l

    // create a thread to accept RPC connections
    go func() {
      for px.isdead() == false {
        conn, err := px.l.Accept()
        if err == nil && px.isdead() == false {
          if px.isunreliable() && (rand.Int63()%1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.isunreliable() && (rand.Int63()%1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            atomic.AddInt32(&px.rpcCount, 1)
            go rpcs.ServeConn(conn)
          } else {
            atomic.AddInt32(&px.rpcCount, 1)
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.isdead() == false {
          px.logger.Debug(DError, "Paxos(%v) accept: %v\n", me, err.Error())
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}

func (px *Paxos) updateInfoFromPeer(s int, seq, done int) *Instance {
  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.checkAndCreateInstance(seq)
  px.extractMinDoneNum(s, done)
  return ins
}

func (px *Paxos) checkAndCreateInstance(seq int) *Instance {
  if px.insts[seq] == nil {
    px.seqs.HPush(seq)
    //px.values[seq] = nil
    px.insts[seq] = NewInstance(px.me, seq, px.peers, px.minDoneSeq[px.me], px.logger, px.updateValueResult)
  }
  return px.insts[seq]
}

func (px *Paxos) extractMinDoneNum(s int, done int) {
  if done > px.minDoneSeq[s] {
    px.minDoneSeq[s] = done
    //px.logger.Debug(DTrck, "Server %d has min done advanced to %d, Min Done Seqs: %v", done, px.minDoneSeq)
  }

}

func (px *Paxos) initMinDoneSeq() {
  for s, _ := range px.peers {
    px.minDoneSeq[s] = -1
  }
}

func (px *Paxos) cleanup() {
  // forget about the old instances < Min
  var m int
  for !px.killed() {
    px.mu.Lock()
    m = px.min()
    for px.seqs.Len() > 0 {
      s := px.seqs.Top().(int)
      if s < m {
        //px.logger.Debug(DTrck, "Remove data for seq %d", s)
        px.seqs.HPop()
        delete(px.values, s)

        if ins, exist := px.insts[s]; exist {
          ins.Kill()
          delete(px.insts, s)
        }

      } else {
        break
      }
    }
    px.mu.Unlock()
    time.Sleep(500 * time.Millisecond)
  }

}
