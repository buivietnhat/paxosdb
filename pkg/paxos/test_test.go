package paxos

import (
  "fmt"
  "math/rand"
  "os"
  "runtime"
  "strconv"
  "sync/atomic"
  "testing"
  "time"

  crand "crypto/rand"
  "encoding/base64"

  . "pxdb/pkg/logger"
)

var logger Logger

func randstring(n int) string {
  b := make([]byte, 2*n)
  crand.Read(b)
  s := base64.URLEncoding.EncodeToString(b)
  return s[0:n]
}

func port(tag string, host int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  os.Mkdir(s, 0777)
  s += "px-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += tag + "-"
  s += strconv.Itoa(host)
  return s
}

func ndecided(t *testing.T, pxa []*Paxos, seq int) int {
  count := 0
  var v interface{}
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      decided, v1 := pxa[i].Status(seq)
      if decided == Decided {
        if count > 0 && v != v1 {
          logger.Debug(DTest, "decided values do not match; seq=%v i=%v v=%v v1=%v", seq, i, v, v1)
          t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
            seq, i, v, v1)
        }
        count++
        v = v1
      }
    }
  }
  return count
}

func waitn(t *testing.T, pxa []*Paxos, seq int, wanted int) {
  to := 10 * time.Millisecond
  for iters := 0; iters < 30; iters++ {
    if ndecided(t, pxa, seq) >= wanted {
      break
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
  }
  nd := ndecided(t, pxa, seq)
  if nd < wanted {
    logger.Debug(DTest, "too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
    t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
  }
}

func waitmajority(t *testing.T, pxa []*Paxos, seq int) {
  waitn(t, pxa, seq, (len(pxa)/2)+1)
}

func checkmax(t *testing.T, pxa []*Paxos, seq int, max int) {
  time.Sleep(3 * time.Second)
  nd := ndecided(t, pxa, seq)
  if nd > max {
    logger.Debug(DTest, "too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
    t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", seq, nd, max)
  }
}

func cleanup(pxa []*Paxos) {
  for i := 0; i < len(pxa); i++ {
    if pxa[i] != nil {
      pxa[i].Kill()
    }
  }
}

func noTestSpeed(t *testing.T) {
  runtime.GOMAXPROCS(4)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("time", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }

  t0 := time.Now()

  for i := 0; i < 20; i++ {
    pxa[0].Start(i, "x")
    waitn(t, pxa, i, npaxos)
  }

  d := time.Since(t0)
  fmt.Printf("20 agreements %v seconds\n", d.Seconds())
  fmt.Printf("1 agreement takes ~ %v miliseconds\n", d.Milliseconds()/20)
}

func TestBasic(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("basic", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }

  logger.Debug(DTest, "Test: Single proposer ...\n")
  fmt.Printf("Test: Single proposer ...\n")

  pxa[0].Start(0, "hello")
  waitn(t, pxa, 0, npaxos)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: Many proposers, same value ...\n")
  fmt.Printf("Test: Many proposers, same value ...\n")

  for i := 0; i < npaxos; i++ {
    pxa[i].Start(1, 77)
  }
  waitn(t, pxa, 1, npaxos)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: Many proposers, different values ...\n")
  fmt.Printf("Test: Many proposers, different values ...\n")

  pxa[0].Start(2, 100)
  pxa[1].Start(2, 101)
  pxa[2].Start(2, 102)
  waitn(t, pxa, 2, npaxos)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: Out-of-order instances ...\n")
  fmt.Printf("Test: Out-of-order instances ...\n")

  pxa[0].Start(7, 700)
  pxa[0].Start(6, 600)
  pxa[1].Start(5, 500)
  waitn(t, pxa, 7, npaxos)
  pxa[0].Start(4, 400)
  pxa[1].Start(3, 300)
  waitn(t, pxa, 6, npaxos)
  waitn(t, pxa, 5, npaxos)
  waitn(t, pxa, 4, npaxos)
  waitn(t, pxa, 3, npaxos)

  if pxa[0].Max() != 7 {
    logger.Debug(DTest, "wrong Max(), expected 7, got %d", pxa[0].Max())
    t.Fatalf("wrong Max()")
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

func TestDeaf(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("deaf", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }

  logger.Debug(DTest, "Test: Deaf proposer ...\n")
  fmt.Printf("Test: Deaf proposer ...\n")

  pxa[0].Start(0, "hello")
  waitn(t, pxa, 0, npaxos)

  os.Remove(pxh[0])
  os.Remove(pxh[npaxos-1])

  pxa[1].Start(1, "goodbye")
  waitmajority(t, pxa, 1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos-2 {
    logger.Debug(DTest, "a deaf peer heard about a decision")
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[0].Start(1, "xxx")
  waitn(t, pxa, 1, npaxos-1)
  time.Sleep(1 * time.Second)
  if ndecided(t, pxa, 1) != npaxos-1 {
    logger.Debug(DTest, "a deaf peer heard about a decision")
    t.Fatalf("a deaf peer heard about a decision")
  }

  pxa[npaxos-1].Start(1, "yyy")
  waitn(t, pxa, 1, npaxos)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

func TestForget(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  const npaxos = 6
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("gc", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }

  logger.Debug(DTest, "Test: Forgetting ...\n")
  fmt.Printf("Test: Forgetting ...\n")

  // initial Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m > 0 {
      logger.Debug(DTest, "wrong initial Min() %v", m)
      t.Fatalf("wrong initial Min() %v", m)
    }
  }

  pxa[0].Start(0, "00")
  pxa[1].Start(1, "11")
  pxa[2].Start(2, "22")
  pxa[0].Start(6, "66")
  pxa[1].Start(7, "77")

  waitn(t, pxa, 0, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      logger.Debug(DTest, "wrong Min() %v; expected 0", m)
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  waitn(t, pxa, 1, npaxos)

  // Min() correct?
  for i := 0; i < npaxos; i++ {
    m := pxa[i].Min()
    if m != 0 {
      logger.Debug(DTest, "wrong Min() %v; expected 0", m)
      t.Fatalf("wrong Min() %v; expected 0", m)
    }
  }

  // everyone Done() -> Min() changes?
  for i := 0; i < npaxos; i++ {
    pxa[i].Done(0)
  }
  for i := 1; i < npaxos; i++ {
    pxa[i].Done(1)
  }
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(8+i, "xx")
  }
  allok := false
  for iters := 0; iters < 12; iters++ {
    allok = true
    for i := 0; i < npaxos; i++ {
      s := pxa[i].Min()
      if s != 1 {
        logger.Debug(DTest, "Iter = %d, min should = 1, but got %d", iters, s)
        allok = false
      }
    }
    if allok {
      break
    }
    time.Sleep(1 * time.Second)
  }
  if allok != true {
    logger.Debug(DTest, "Min() did not advance after Done()")
    t.Fatalf("Min() did not advance after Done()")
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

func TestManyForget(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("manygc", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
    pxa[i].setunreliable(true)
  }

  logger.Debug(DTest, "Test: Lots of forgetting ...\n")
  fmt.Printf("Test: Lots of forgetting ...\n")

  const maxseq = 20

  go func() {
    na := rand.Perm(maxseq)
    for i := 0; i < len(na); i++ {
      seq := na[i]
      j := (rand.Int() % npaxos)
      v := rand.Int()
      pxa[j].Start(seq, v)
      runtime.Gosched()
    }
  }()

  done := make(chan bool)
  go func() {
    for {
      select {
      case <-done:
        return
      default:
      }
      seq := (rand.Int() % maxseq)
      i := (rand.Int() % npaxos)
      if seq >= pxa[i].Min() {
        decided, _ := pxa[i].Status(seq)
        if decided == Decided {
          pxa[i].Done(seq)
        }
      }
      runtime.Gosched()
    }
  }()

  time.Sleep(5 * time.Second)
  done <- true
  for i := 0; i < npaxos; i++ {
    pxa[i].setunreliable(false)
  }
  time.Sleep(2 * time.Second)

  for seq := 0; seq < maxseq; seq++ {
    for i := 0; i < npaxos; i++ {
      if seq >= pxa[i].Min() {
        pxa[i].Status(seq)
      }
    }
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

//
// does Max() work after Done()s?
//
func TestDoneMax(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  logger.Debug(DTest, "Test: Paxos Max() after Done()s ...\n")
  fmt.Printf("Test: Paxos Max() after Done()s ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("donemax", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }

  pxa[0].Start(0, "x")
  waitn(t, pxa, 0, npaxos)

  for i := 1; i <= 10; i++ {
    pxa[0].Start(i, "y")
    waitn(t, pxa, i, npaxos)
  }

  for i := 0; i < npaxos; i++ {
    pxa[i].Done(10)
  }

  // Propagate messages so everyone knows about Done(10)
  for i := 0; i < npaxos; i++ {
    pxa[i].Start(10, "z")
  }
  time.Sleep(2 * time.Second)
  for i := 0; i < npaxos; i++ {
    mx := pxa[i].Max()
    if mx != 10 {
      logger.Debug(DTest, "Max() did not return correct result %d after calling Done(); returned %d", 10, mx)
      t.Fatalf("Max() did not return correct result %d after calling Done(); returned %d", 10, mx)
    }
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

//many agreements (without failures)

func TestMany(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  logger.Debug(DTest, "Test: Many instances ...\n")
  fmt.Printf("Test: Many instances ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("many", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 5 active instances, to limit the
    // number of file descriptors.
    for seq >= 5 && ndecided(t, pxa, seq-5) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq*10)+i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
//
func TestOld(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  logger.Debug(DTest, "Test: Minority proposal ignored ...\n")
  fmt.Printf("Test: Minority proposal ignored ...\n")

  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("old", i)
  }

  l1 := NewServerLogger(1)
  pxa[1] = Make(pxh, 1, nil, l1)
  l2 := NewServerLogger(2)
  pxa[2] = Make(pxh, 2, nil, l2)
  l3 := NewServerLogger(3)
  pxa[3] = Make(pxh, 3, nil, l3)
  pxa[1].Start(1, 111)

  waitmajority(t, pxa, 1)

  l0 := NewServerLogger(0)
  pxa[0] = Make(pxh, 0, nil, l0)
  pxa[0].Start(1, 222)

  waitn(t, pxa, 1, 4)

  if false {
    l4 := NewServerLogger(4)
    pxa[4] = Make(pxh, 4, nil, l4)
    waitn(t, pxa, 1, npaxos)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  logger.Debug(DTest, "Test: Many instances, unreliable RPC ...\n")
  fmt.Printf("Test: Many instances, unreliable RPC ...\n")

  const npaxos = 3
  var pxa []*Paxos = make([]*Paxos, npaxos)
  var pxh []string = make([]string, npaxos)
  defer cleanup(pxa)

  for i := 0; i < npaxos; i++ {
    pxh[i] = port("manyun", i)
  }
  for i := 0; i < npaxos; i++ {
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
    pxa[i].setunreliable(true)
    pxa[i].Start(0, 0)
  }

  const ninst = 50
  for seq := 1; seq < ninst; seq++ {
    // only 3 active instances, to limit the
    // number of file descriptors.
    for seq >= 3 && ndecided(t, pxa, seq-3) < npaxos {
      time.Sleep(20 * time.Millisecond)
    }
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq*10)+i)
    }
  }

  for {
    done := true
    for seq := 1; seq < ninst; seq++ {
      if ndecided(t, pxa, seq) < npaxos {
        done = false
      }
    }
    if done {
      break
    }
    time.Sleep(100 * time.Millisecond)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

func pp(tag string, src int, dst int) string {
  s := "/var/tmp/824-"
  s += strconv.Itoa(os.Getuid()) + "/"
  s += "px-" + tag + "-"
  s += strconv.Itoa(os.Getpid()) + "-"
  s += strconv.Itoa(src) + "-"
  s += strconv.Itoa(dst)
  return s
}

func cleanpp(tag string, n int) {
  for i := 0; i < n; i++ {
    for j := 0; j < n; j++ {
      ij := pp(tag, i, j)
      os.Remove(ij)
    }
  }
}

func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
  cleanpp(tag, npaxos)

  pa := [][]int{p1, p2, p3}
  for pi := 0; pi < len(pa); pi++ {
    p := pa[pi]
    for i := 0; i < len(p); i++ {
      for j := 0; j < len(p); j++ {
        ij := pp(tag, p[i], p[j])
        pj := port(tag, p[j])
        err := os.Link(pj, ij)
        if err != nil {
          // one reason this link can fail is if the
          // corresponding Paxos peer has prematurely quit and
          // deleted its socket file (e.g., called px.Kill()).
          t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
        }
      }
    }
  }
}

func TestPartition(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  tag := "partition"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  seq := 0

  logger.Debug(DTest, "Test: No decision if partitioned ...\n")
  fmt.Printf("Test: No decision if partitioned ...\n")

  part(t, tag, npaxos, []int{0, 2}, []int{1, 3}, []int{4})
  pxa[1].Start(seq, 111)
  checkmax(t, pxa, seq, 0)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: Decision in majority partition ...\n")
  fmt.Printf("Test: Decision in majority partition ...\n")

  part(t, tag, npaxos, []int{0}, []int{1, 2, 3}, []int{4})
  time.Sleep(2 * time.Second)
  waitmajority(t, pxa, seq)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: All agree after full heal ...\n")
  fmt.Printf("Test: All agree after full heal ...\n")

  pxa[0].Start(seq, 1000) // poke them
  pxa[4].Start(seq, 1004)
  part(t, tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{})

  waitn(t, pxa, seq, npaxos)

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: One peer switches partitions ...\n")
  fmt.Printf("Test: One peer switches partitions ...\n")

  for iters := 0; iters < 20; iters++ {
    seq++

    part(t, tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{})
    pxa[0].Start(seq, seq*10)
    pxa[3].Start(seq, (seq*10)+1)
    waitmajority(t, pxa, seq)
    if ndecided(t, pxa, seq) > 3 {
      logger.Debug(DTest, "too many decided")
      t.Fatalf("too many decided")
    }

    part(t, tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{})
    waitn(t, pxa, seq, npaxos)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")

  logger.Debug(DTest, "Test: One peer switches partitions, unreliable ...\n")
  fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

  for iters := 0; iters < 20; iters++ {
    seq++

    for i := 0; i < npaxos; i++ {
      pxa[i].setunreliable(true)
    }

    part(t, tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{})
    for i := 0; i < npaxos; i++ {
      pxa[i].Start(seq, (seq*10)+i)
    }
    waitn(t, pxa, seq, 3)
    if ndecided(t, pxa, seq) > 3 {
      logger.Debug(DTest, "too many decided")
      t.Fatalf("too many decided")
    }

    part(t, tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{})

    for i := 0; i < npaxos; i++ {
      pxa[i].setunreliable(false)
    }

    waitn(t, pxa, seq, 5)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}

func TestLots(t *testing.T) {
  runtime.GOMAXPROCS(4)
  logger = NewServerLogger(-1)

  logger.Debug(DTest, "Test: Many requests, changing partitions ...\n")
  fmt.Printf("Test: Many requests, changing partitions ...\n")

  tag := "lots"
  const npaxos = 5
  var pxa []*Paxos = make([]*Paxos, npaxos)
  defer cleanup(pxa)
  defer cleanpp(tag, npaxos)

  for i := 0; i < npaxos; i++ {
    var pxh []string = make([]string, npaxos)
    for j := 0; j < npaxos; j++ {
      if j == i {
        pxh[j] = port(tag, i)
      } else {
        pxh[j] = pp(tag, i, j)
      }
    }
    sLogger := NewServerLogger(i)
    pxa[i] = Make(pxh, i, nil, sLogger)
    pxa[i].setunreliable(true)
  }
  defer part(t, tag, npaxos, []int{}, []int{}, []int{})

  done := int32(0)

  // re-partition periodically
  ch1 := make(chan bool)
  go func() {
    defer func() { ch1 <- true }()
    for atomic.LoadInt32(&done) == 0 {
      var a [npaxos]int
      for i := 0; i < npaxos; i++ {
        a[i] = (rand.Int() % 3)
      }
      pa := make([][]int, 3)
      for i := 0; i < 3; i++ {
        pa[i] = make([]int, 0)
        for j := 0; j < npaxos; j++ {
          if a[j] == i {
            pa[i] = append(pa[i], j)
          }
        }
      }
      part(t, tag, npaxos, pa[0], pa[1], pa[2])
      time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
    }
  }()

  seq := int32(0)

  // periodically start a new instance
  ch2 := make(chan bool)
  go func() {
    defer func() { ch2 <- true }()
    for atomic.LoadInt32(&done) == 0 {
      // how many instances are in progress?
      nd := 0
      sq := int(atomic.LoadInt32(&seq))
      for i := 0; i < sq; i++ {
        if ndecided(t, pxa, i) == npaxos {
          nd++
        }
      }
      if sq-nd < 10 {
        for i := 0; i < npaxos; i++ {
          pxa[i].Start(sq, rand.Int()%10)
        }
        atomic.AddInt32(&seq, 1)
      }
      time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
    }
  }()

  // periodically check that decisions are consistent
  ch3 := make(chan bool)
  go func() {
    defer func() { ch3 <- true }()
    for atomic.LoadInt32(&done) == 0 {
      for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
        ndecided(t, pxa, i)
      }
      time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
    }
  }()

  time.Sleep(20 * time.Second)
  atomic.StoreInt32(&done, 1)
  <-ch1
  <-ch2
  <-ch3

  // repair, then check that all instances decided.
  for i := 0; i < npaxos; i++ {
    pxa[i].setunreliable(false)
  }
  part(t, tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{})
  time.Sleep(5 * time.Second)

  for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
    waitmajority(t, pxa, i)
  }

  logger.Debug(DTest, "  ... Passed\n")
  fmt.Printf("  ... Passed\n")
}
