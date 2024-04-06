package kvpaxos

import (
  "container/list"
  "encoding/gob"
  "errors"
  "fmt"
  "log"
  "math/rand"
  "net"
  "net/rpc"
  "os"
  . "pxdb/pkg/logger"
  "pxdb/pkg/paxos"
  "sync"
  "sync/atomic"
  "syscall"
  "time"
)

const timeout = 2

type KVPaxos struct {
  l          net.Listener
  me         int
  dead       int32 // for testing
  unreliable int32 // for testing
  px         *paxos.Paxos

  db     Storer[string, string]
  filter Filter[FilterKey, *OpResult]

  logger Logger
  done   chan struct{}

  lastAppl int
  appList  *list.List

  mu  sync.Mutex
  seq int

  wait map[int]chan *OpResult
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.logger.Debug(DServ, "Receive Get request from %d xid %d key %v", shrink(args.Uuid), shrink(args.Xid), args.Key)

  // have I served this request before?
  served, r, err := kv.filter.Exist(NewFilterKey(args.Uuid, args.Xid))
  if err == nil && served {
    kv.logger.Debug(DDupl, "Request Get from %d xid %d key %v already served, return ...", shrink(args.Uuid), shrink(args.Xid), args.Key)
    reply.Err = OK
    reply.Value = r.value
  }

  for !kv.isdead() {
    pmax := kv.px.Max() + 1

    kv.mu.Lock()
    if kv.seq < pmax {
      kv.seq = pmax
    } else {
      kv.seq += 1
    }
    s := kv.seq

    ch := make(chan *OpResult)
    kv.wait[s] = ch
    kv.mu.Unlock()

    op := newOp(Get, args.Key, "", args.Xid, s, args.Uuid)

    if !kv.px.Start(s, *op) {
      kv.logger.Debug(DTrck, "Coundn't get agreement for Get from %d xid %d key %v, retry with a new instance",
        shrink(args.Uuid), shrink(args.Xid), args.Key)
      continue
    }

    var cmitop *OpResult
    for {
      select {
      case cmitop = <-ch:
        goto OK
      case <-kv.done:
        return errors.New("got killed")
      case <-time.After(timeout * time.Second):
        kv.logger.Debug(DError, "Couldn't get aggreement for seq %d after %d second, try retrieving missing ones", s, timeout)
        go kv.tryRetrieveMissingSeq(s)
      }
    }

  OK:
    // is the cmitop is the op I've tried to have an aggrement?
    if !op.equal(cmitop.op) {
      if cmitop != nil {
        kv.logger.Debug(DTrck, "Couldn't get aggreement for seq %d, retry with new instance", s)
      }
      continue
    }

    if cmitop.err != nil {
      kv.logger.Debug(DWarn, "err %v serving Get request for key %v for client %d", cmitop.err, args.Key, shrink(args.Uuid))
      reply.Err = ErrNoKey
      return nil
    }

    kv.logger.Debug(DServ, "Got value %v for key %v for client %d", cmitop.value, args.Key, shrink(args.Uuid))
    reply.Value = cmitop.value
    reply.Err = OK
    break
  }

  return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
  kv.logger.Debug(DServ, "Receive %v request from %d xid %d key %v value %v", args.Op, shrink(args.Uuid), shrink(args.Xid), args.Key, args.Value)

  // have I served this request before?
  served, _, err := kv.filter.Exist(NewFilterKey(args.Uuid, args.Xid))
  if err == nil && served {
    kv.logger.Debug(DDupl, "Request %v from %d xid %d key %v value %v already served, return ...",
      args.Op, shrink(args.Uuid), shrink(args.Xid), args.Key, args.Value)
    reply.Err = OK
    return nil
  }

  for !kv.isdead() {
    pmax := kv.px.Max() + 1

    kv.mu.Lock()
    if kv.seq < pmax {
      kv.seq = pmax
    } else {
      kv.seq += 1
    }
    s := kv.seq

    ch := make(chan *OpResult)
    kv.wait[s] = ch
    kv.mu.Unlock()

    op := newOp(args.Op, args.Key, args.Value, args.Xid, s, args.Uuid)

    if !kv.px.Start(s, *op) {
      kv.logger.Debug(DTrck, "Coundn't get agreement for Get from %d xid %d key %v seq %d, retry with a new instance",
        shrink(args.Uuid), shrink(args.Xid), args.Key, s)
      continue
    }

    var cmitop *OpResult
    for {
      select {
      case cmitop = <-ch:
        goto OK
      case <-kv.done:
        return errors.New("got killed")
      case <-time.After(timeout * time.Second):
        kv.logger.Debug(DError, "Couldn't get aggreement for seq %d after %d second, try retrieving missing ones", s, timeout)
        go kv.tryRetrieveMissingSeq(s)
      }
    }

  OK:
    // is the cmitop is the op I've tried to have an aggrement?
    if !op.equal(cmitop.op) {
      if cmitop != nil {
        kv.logger.Debug(DTrck, "Couldn't get aggreement for seq %d, retry with new instance", s)
      }
      continue
    }

    if cmitop.err != nil {
      kv.logger.Debug(DWarn, "err %v serving %v request for key %v for client %d", args.Op, cmitop.err, args.Key, shrink(args.Uuid))
      reply.Err = Other
      return nil
    }

    kv.logger.Debug(DServ, "Served %v request for key %v value %v OK for client %d", args.Op, args.Key, args.Value, shrink(args.Uuid))
    reply.Err = OK

    break
  }

  return nil
}

func (kv *KVPaxos) kill() {
  atomic.StoreInt32(&kv.dead, 1)
  kv.l.Close()
  kv.px.Kill()
  close(kv.done)
}

func (kv *KVPaxos) isdead() bool {
  return atomic.LoadInt32(&kv.dead) != 0
}

func (kv *KVPaxos) setunreliable(what bool) {
  kv.logger.Debug(DTrck, "setunreliable set to %v", what)
  if what {
    atomic.StoreInt32(&kv.unreliable, 1)
  } else {
    atomic.StoreInt32(&kv.unreliable, 0)
  }
}

func (kv *KVPaxos) isunreliable() bool {
  return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int, logger Logger) *KVPaxos {
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  kv.appList = list.New()
  kv.done = make(chan struct{})
  kv.logger = logger
  kv.wait = make(map[int]chan *OpResult)
  kv.db = NewHashDb()
  kv.filter = NewDoubleHashFilter[int64, int64, FilterKey, *OpResult]()

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs, logger)

  go kv.applyProcess()

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me])
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  kv.l = l

  go func() {
    for kv.isdead() == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.isdead() == false {
        if kv.isunreliable() && (rand.Int63()%1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.isdead() == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

func (kv *KVPaxos) applyOp(op *Op) *OpResult {
  var (
    v   string = ""
    err error  = errors.New("unrecognized op")
  )

  //kv.logger.Debug(DServ, "applying op seq %d", op.Seq)

  //have I installed this op before?
  fkey := NewFilterKey(op.Uuid, op.Xid)
  installed, r, e := kv.filter.Exist(fkey)
  if e == nil && installed {
    kv.logger.Debug(DDupl, "Op %v from %d xid %d key %v value %v already installed, return ...",
      op.Type, shrink(op.Uuid), shrink(op.Xid), op.Key, op.Value)
    return r
  }

  switch op.Type {
  case Get:
    v, err = kv.db.Get(op.Key)
  case Put:
    err = kv.db.Put(op.Key, op.Value)
  case Append:
    err = kv.db.Append(op.Key, op.Value)
  case Nop:
  default:
    panic("unreconized op")
  }

  or := NewOpResult(op, v, err)
  kv.filter.Record(fkey, or)

  return or
}

func (kv *KVPaxos) Done(seq int) {
  go kv.px.Done(seq)

  kv.setLastApplied(seq)
}

func (kv *KVPaxos) applyAndShrink() {
  var (
    next    *list.Element
    lastApl int = kv.LastApplied()
  )

  for e := kv.appList.Front(); e != nil; e = next {
    op := e.Value.(*Op)
    if op.Seq == lastApl+1 {
      or := kv.applyOp(op)

      lastApl += 1
      next = e.Next()
      kv.appList.Remove(e)

      // notify for waiting channel
      kv.mu.Lock()
      ch, waiting := kv.wait[op.Seq]
      kv.mu.Unlock()

      if waiting {
        select {
        case ch <- or:
        default:
        }
      }

    } else {
      break
    }
  }

  // kv.printApplist()

  // has processed some new instances, call Done to free up memory
  if lastApl > kv.LastApplied() {
    kv.Done(lastApl)
  }
}

func (kv *KVPaxos) checkApplyAndShrink(op *Op) {
  for e := kv.appList.Front(); e != nil; e = e.Next() {
    if e.Value.(*Op).Seq > op.Seq {
      kv.appList.InsertBefore(op, e)
      goto APPLY
    }
  }
  kv.appList.PushBack(op)

APPLY:
  kv.applyAndShrink()
}

func (kv *KVPaxos) applyProcess() {
  var (
    to  time.Duration = 10 * time.Millisecond
    seq int           = 1
  )

  for !kv.isdead() {
    select {
    case <-kv.done:
      return
    default:
    }

    status, v := kv.px.Status(seq)
    if status == paxos.Decided {
      op, ok := v.(Op)
      if !ok {
        panic("coundn't parse the Op")
      }

      // would launching a new goroutine here be faster?
      kv.checkApplyAndShrink(&op)

      seq += 1
      to = 10 * time.Millisecond
      continue
    }

    time.Sleep(to)
    if to < 10*time.Second {
      to *= 2
    }
  }
}

func (kv *KVPaxos) tryRetrieveMissingSeq(upto int) {
  lastAppl := kv.LastApplied()
  for s := lastAppl + 1; s < upto; s++ {
    op := newOp(Nop, "", "", 0, s, 0)
    kv.px.Start(s, *op)
  }
}

func (kv *KVPaxos) LastApplied() int {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  return kv.lastAppl
}

func (kv *KVPaxos) setLastApplied(seq int) {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.lastAppl = seq
}

func (kv *KVPaxos) printApplist() {
  var list string
  for e := kv.appList.Front(); e != nil; e = e.Next() {
    list += fmt.Sprintf("%d ", e.Value.(*Op).Seq)
  }
  kv.logger.Debug(DServ, "app list: %s", list)
}
