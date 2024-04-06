package paxos

type Err int32

const (
  LossConnection Err = iota
  Ok
  Rejected
)

func String(e Err) string {
  switch e {
  case LossConnection:
    return "LossConnection"
  case Ok:
    return "Ok"
  case Rejected:
    return "Rejected"
  }
  return ""
}

type PrepareArgs struct {
  S    int
  N    Np
  Seq  int
  Done int
}

type PrepareReply struct {
  Err Err
  Np  Np
  Na  Np
  Va  interface{}
}

type AcceptArgs struct {
  S    int
  N    Np
  V    interface{}
  Seq  int
  Done int
}

type AcceptReply struct {
  Err Err
  Na  Np
  Np  Np
}

type DecideArgs struct {
  S    int
  V    interface{}
  Seq  int
  Done int
}

type DecideReply struct {
  Err Err
}
