package kvpaxos

import (
  "crypto/rand"
  "math/big"
  "net/rpc"
  "sync/atomic"
  "time"

  "fmt"
  . "pxdb/pkg/logger"
)

type Clerk struct {
  servers []string
  uuid    int64
  xid     int64
  l       Logger
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  ck.uuid = nrand()
  ck.l = NewServerLogger(int(shrink(ck.uuid)))
  return ck
}

func call(srv string, rpcname string,
    args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  xid := atomic.AddInt64(&ck.xid, 1)
  args := GetArgs{Key: key, Uuid: ck.uuid, Xid: xid}
  reply := GetReply{}

  ck.l.Debug(DCler, "Request Get for key: %v xid %d ", key, shrink(xid))

  for {
    for _, sname := range ck.servers {
      if call(sname, "KVPaxos.Get", &args, &reply) && (reply.Err == OK || reply.Err == ErrNoKey) {
        ck.l.Debug(DCler, "Request Get for key: %v xid %d  OK with value %v", key, shrink(xid), reply.Value)
        return reply.Value
      } else {
        ck.l.Debug(DCler, "Request Get for key: %v xid %d Failed with err %v", key, shrink(xid), reply.Err)
      }

      time.Sleep(10 * time.Millisecond)
    }
  }

  return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op Type) {
  xid := atomic.AddInt64(&ck.xid, 1)
  args := PutAppendArgs{Key: key, Uuid: ck.uuid, Xid: xid, Op: op, Value: value}
  reply := PutAppendReply{}

  ck.l.Debug(DCler, "Request %v for key: %v value: %v xid %d ", op, key, value, shrink(xid))

  for {
    for _, sname := range ck.servers {
      if call(sname, "KVPaxos.PutAppend", &args, &reply) && reply.Err == OK {
        ck.l.Debug(DCler, "Request %v for key: %v value: %v xid %d OK", op, key, value, shrink(xid))
        return
      } else {
        ck.l.Debug(DCler, "Request Get for key: %v value: %v xid %d Failed with err %v", key, value, shrink(xid), reply.Err)
      }

      time.Sleep(10 * time.Millisecond)
    }
  }

}

func (ck *Clerk) Put(key string, value string) {
  ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
  ck.PutAppend(key, value, Append)
}
