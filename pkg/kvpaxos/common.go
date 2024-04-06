package kvpaxos

import "log"

type Err string

const (
  OK       Err = "OK"
  ErrNoKey     = "ErrNoKey"
  Other        = "Other"
)

// Put or Append
type PutAppendArgs struct {
  Key   string
  Value string
  Op    Type // "Put" or "Append"
  Uuid  int64
  Xid   int64
}

type PutAppendReply struct {
  Err Err
}

type GetArgs struct {
  Key  string
  Uuid int64
  Xid  int64
}

type GetReply struct {
  Err   Err
  Value string
}

func shrink(x int64) int64 {
  return x % 10000
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Type string

const (
  Get    Type = "Get"
  Put         = "Put"
  Append      = "Append"
  Nop         = "Nop"
)

type Op struct {
  Type  Type
  Key   string
  Value string
  Xid   int64
  Uuid  int64
  Seq   int
  //Ch    chan *OpResult
}

func (o *Op) equal(other *Op) bool {
  return o.Type == other.Type && o.Key == other.Key && o.Value == other.Value && o.Xid == other.Xid && o.Uuid == other.Uuid
}

type OpResult struct {
  op    *Op
  value string
  err   error
}

func NewOpResult(op *Op, value string, err error) *OpResult {
  return &OpResult{op: op, value: value, err: err}
}

func newOp(t Type, key string, value string, xid int64, seq int, uuid int64) *Op {
  op := &Op{}
  op.Type = t
  op.Key = key
  op.Value = value
  op.Xid = xid
  op.Seq = seq
  op.Uuid = uuid
  //op.Ch = make(chan *OpResult)
  return op
}

type FilterKey struct {
  uuid int64
  xid  int64
}

func NewFilterKey(uuid int64, xid int64) FilterKey {
  return FilterKey{uuid: uuid, xid: xid}
}

func (fk FilterKey) First() int64 {
  return fk.uuid
}

func (fk FilterKey) Second() int64 {
  return fk.xid
}

//func Min[T comparable](a, b T) T {
//  if a > b {
//    return b
//  }
//  return a
//}
