package logger

import (
  "fmt"
  "log"
  "os"
  "strconv"
  "time"
)

type LogTopic string

var debugStart time.Time
var debugVerbosity int

const (
  DClient   LogTopic = "CLNT"
  DCommit   LogTopic = "CMIT"
  DDrop     LogTopic = "DROP"
  DError    LogTopic = "ERRO"
  DInfo     LogTopic = "INFO"
  DDecide   LogTopic = "LEAD"
  DLeader1  LogTopic = "LED1"
  DProposer LogTopic = "PROP"
  DAcceptor LogTopic = "ACEP"
  DPersist  LogTopic = "PERS"
  DSnap     LogTopic = "SNAP"
  DSnp1     LogTopic = "SNP1"
  DTerm     LogTopic = "TERM"
  DTest     LogTopic = "TEST"
  DTimer    LogTopic = "TIMR"
  DTrace    LogTopic = "TRCE"
  DVote     LogTopic = "VOTE"
  DWarn     LogTopic = "WARN"
  DServ     LogTopic = "SERV"
  DCler     LogTopic = "CLER"
  DDupl     LogTopic = "DUPL"
  DTrck     LogTopic = "TRCK"
  DShardCtr LogTopic = "SCTR"
)

type Logger interface {
  Debug(topic LogTopic, format string, a ...interface{})
}

type ServerLogger struct {
  me int
}

func NewServerLogger(me int) *ServerLogger {
  logger := &ServerLogger{
    me: me,
  }
  return logger
}

type InstanceLogger struct {
  me  int
  seq int
}

func NewInstanceLogger(me int, seq int) *InstanceLogger {
  return &InstanceLogger{me: me, seq: seq}
}

func (il *InstanceLogger) Debug(topic LogTopic, format string, a ...interface{}) {
  if debugVerbosity >= 1 {
    time := time.Since(debugStart).Microseconds()
    time /= 1000
    var prefix string

    prefix = fmt.Sprintf("%06d %v [%d] {%d} ", time, string(topic), il.me, il.seq)

    format = prefix + format + "\n"
    fmt.Printf(format, a...)
  }
}

func (sl *ServerLogger) Debug(topic LogTopic, format string, a ...interface{}) {
  if debugVerbosity >= 1 {
    time := time.Since(debugStart).Microseconds()
    time /= 1000
    var prefix string
    if sl.me == -1 {
      prefix = fmt.Sprintf("%06d %v ", time, string(topic))
    } else {
      prefix = fmt.Sprintf("%06d %v [%d] ", time, string(topic), sl.me)
    }

    format = prefix + format + "\n"
    fmt.Printf(format, a...)
  }
}

type GroupLogger struct {
  gid    int
  server int
}

func NewGroupLogger(gid int, server int) *GroupLogger {
  return &GroupLogger{gid: gid, server: server}
}

func (gl *GroupLogger) Debug(topic LogTopic, format string, a ...interface{}) {
  if debugVerbosity >= 1 {
    time := time.Since(debugStart).Microseconds()
    time /= 1000
    var prefix string

    prefix = fmt.Sprintf("%06d %v [%d](%d) ", time, string(topic), gl.gid, gl.server)

    format = prefix + format + "\n"
    fmt.Printf(format, a...)
  }
}

func init() {
  debugVerbosity = getVerbosity()
  debugStart = time.Now()
  log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
  v := os.Getenv("VERBOSE")
  level := 0
  if v != "" {
    var err error
    level, err = strconv.Atoi(v)
    if err != nil {
      log.Fatalf("Invalid verbosity %v", v)
    }
  }
  return level
}

func DebugEnabled() bool {
  return debugVerbosity >= 1
}
