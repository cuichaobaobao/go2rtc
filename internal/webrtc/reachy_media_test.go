package webrtc

import (
 "bytes"
 "context"
 "fmt"
 "net"
 "os"
 "os/exec"
 "runtime"
 "strconv"
 "strings"
 "sync"
 "testing"
 "time"

 "github.com/AlexxIT/go2rtc/pkg/core"
 "github.com/AlexxIT/go2rtc/pkg/rtsp"
 "github.com/pion/rtp"
 "github.com/pion/rtp/codecs"
 pion "github.com/pion/webrtc/v4"
)

// FFmpeg is only a cloud-test fixture generator and independent decoder.
// The adapter itself never invokes a decoder.
func TestReachyPlaybackRTSP(t *testing.T) {
 if os.Getenv("REACHY_MEDIA_TEST") != "1" { t.Skip("requires cloud FFmpeg") }
 testReachyRTP(t, false, false, true)
}

func frameHashes(b []byte) []string {
 var hashes []string
 for _, line := range strings.Split(string(b), "\n") {
  if line == "" || strings.HasPrefix(line, "#") { continue }
  fields := strings.Split(line, ",")
  if len(fields) == 6 { hashes = append(hashes, strings.TrimSpace(fields[5])) }
 }
 return hashes
}

func testReachyPlayback(t *testing.T, p *reachyProducer, track *pion.TrackLocalStaticRTP) {
 ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
 defer cancel()
 fixture, err := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-f", "lavfi", "-i",
  "testsrc2=size=1280x720:rate=10", "-frames:v", "1", "-c:v", "libx264", "-threads", "1",
  "-profile:v", "baseline", "-level:v", "3.1", "-preset", "ultrafast", "-tune", "zerolatency",
  "-x264-params", "keyint=1:repeat-headers=1", "-f", "h264", "pipe:1").Output()
 if err != nil { t.Fatal(err) }
 ref := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-f", "h264", "-i", "pipe:0",
  "-threads", "1", "-f", "framemd5", "pipe:1")
 ref.Stdin = bytes.NewReader(fixture)
 expected, err := ref.Output()
 if err != nil { t.Fatal(err) }
 hashes := frameHashes(expected)
 if len(hashes) != 1 { t.Fatalf("reference frames: %d", len(hashes)) }

 media := p.GetMedias()[0]
 receiver, err := p.GetTrack(media, media.Codecs[0])
 if err != nil { t.Fatal(err) }
 ln, err := net.Listen("tcp", "127.0.0.1:0")
 if err != nil { t.Fatal(err) }
 var wg sync.WaitGroup
 var connsMu sync.Mutex
 var conns []net.Conn
 acceptDone := make(chan struct{})
 go func() {
  defer close(acceptDone)
  for {
   nc, e := ln.Accept()
   if e != nil { return }
   connsMu.Lock()
   conns = append(conns, nc)
   connsMu.Unlock()
   wg.Add(1)
   go func() {
    defer wg.Done()
    c := rtsp.NewServer(nc)
    defer c.Stop()
    defer c.Close()
    c.Listen(func(msg any) {
     if msg == rtsp.MethodDescribe {
      m := &core.Media{Kind: core.KindVideo, Direction: core.DirectionSendonly, Codecs: media.Codecs}
      if e := c.AddTrack(m, media.Codecs[0], receiver); e != nil { t.Error(e) }
     }
    })
    if c.Accept() == nil { _ = c.Handle() }
   }()
  }
 }()
 defer func() {
  _ = ln.Close()
  <-acceptDone
  connsMu.Lock()
  for _, nc := range conns { _ = nc.Close() }
  connsMu.Unlock()
  wg.Wait()
 }()
 startedDone := make(chan struct{})
 go func() { _ = p.Start(); close(startedDone) }()
 defer func() { _ = p.Stop(); <-startedDone }()

 sentDone := make(chan struct{})
 stopSend := make(chan struct{})
 go func() {
  defer close(sentDone)
  packetizer := rtp.NewPacketizer(1200, 96, 12345, &codecs.H264Payloader{}, rtp.NewRandomSequencer(), 90000)
  ticker := time.NewTicker(100*time.Millisecond)
  defer ticker.Stop()
  for {
   select {
   case <-stopSend: return
   case <-ticker.C:
    for _, packet := range packetizer.Packetize(fixture, 9000) {
     if track.WriteRTP(packet) != nil { return }
    }
   }
  }
 }()
 defer func() { close(stopSend); <-sentDone }()
 type result struct { data []byte; err error; stderr string }
 results := make(chan result, 2)
 for i:=0; i<2; i++ {
  go func() {
   cmd := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-xerror", "-rtsp_transport", "tcp",
    "-i", "rtsp://"+ln.Addr().String()+"/reachy", "-an", "-threads", "1",
    "-frames:v", "300", "-f", "framemd5", "pipe:1")
   var stderr bytes.Buffer
   cmd.Stderr = &stderr
   data, e := cmd.Output()
   results <- result{data,e,stderr.String()}
  }()
 }
 for i:=0; i<2; i++ {
  r := <-results
  if r.err != nil { t.Fatalf("RTSP decode: %v %s", r.err, r.stderr) }
  got := frameHashes(r.data)
  if len(got) != 300 { t.Fatalf("frames=%d",len(got)) }
  if !strings.Contains(string(r.data), "1280x720") { t.Fatal("wrong decoded resolution") }
  for _, hash := range got {
   if hash != hashes[0] { t.Fatal("decoded pixels changed during forwarding") }
  }
  t.Logf("RTSP viewer %d: 300 frames, 1280x720, all pixel hashes equal source", i+1)
 }
}

type reachyResources struct { Heap uint64; RSS, Threads, FDs, Goroutines int }
func sampleReachyResources() reachyResources {
 var m runtime.MemStats
 runtime.ReadMemStats(&m)
 s := reachyResources{Heap:m.HeapAlloc, Goroutines:runtime.NumGoroutine()}
 if entries, err := os.ReadDir("/proc/self/fd"); err == nil { s.FDs = len(entries) }
 status, _ := os.ReadFile("/proc/self/status")
 for _, line := range strings.Split(string(status),"\n") {
  f := strings.Fields(line)
  if len(f)<2 { continue }
  n,_ := strconv.Atoi(f[1])
  switch f[0] { case "VmRSS:": s.RSS=n; case "Threads:": s.Threads=n }
 }
 return s
}

func TestReachyResourceCycles(t *testing.T) {
 if os.Getenv("REACHY_MEDIA_TEST") != "1" { t.Skip("cloud resource run") }
 for i:=0;i<3;i++ { testReachyRTP(t,false,false) }
 time.Sleep(200*time.Millisecond)
 baseline := sampleReachyResources()
 t.Logf("warm baseline: %+v",baseline)
 for i:=0;i<30;i++ {
  testReachyRTP(t,false,i%2==0)
  if (i+1)%5==0 { t.Logf("cycle %d: %+v",i+1,sampleReachyResources()) }
 }
 time.Sleep(time.Second)
 end := sampleReachyResources()
 t.Logf("final: %+v (RSS KiB, heap bytes; no forced GC)",end)
 if end.FDs>baseline.FDs+3 || end.Goroutines>baseline.Goroutines+5 {
  t.Fatal(fmt.Sprintf("resource accumulation: before=%+v after=%+v",baseline,end))
 }
 // RSS includes Go runtime retention; this catches large growth, not all leaks.
 if end.RSS>baseline.RSS+64*1024 { t.Fatal("RSS grew more than 64 MiB across 30 cycles") }
}
