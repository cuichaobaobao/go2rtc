package webrtc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	pion "github.com/pion/webrtc/v4"
)

func TestReachyMissingProducer(t *testing.T) {
	ended := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		ws, err := (&websocket.Upgrader{}).Upgrade(w,r,nil)
		if err != nil { return }
		defer ws.Close()
		_ = ws.WriteJSON(map[string]any{"type":"welcome"})
		var msg reachyMessage
		_ = ws.ReadJSON(&msg)
		if msg.Type != "list" { t.Error("expected list") }
		_ = ws.WriteJSON(map[string]any{"type":"list","producers":[]any{}})
		_ = ws.ReadJSON(&msg)
	}))
	defer s.Close()
	_, err := dialReachy("ws"+strings.TrimPrefix(s.URL,"http"),"reachymini",time.Second)
	if err == nil || !strings.Contains(err.Error(),"not found") { t.Fatalf("unexpected error: %v",err) }
	select { case <-ended: case <-time.After(time.Second): t.Fatal("socket leaked") }
}

func TestReachySilentServerTimeout(t *testing.T) {
	ended := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		ws, err := (&websocket.Upgrader{}).Upgrade(w,r,nil)
		if err != nil { return }
		defer ws.Close()
		var msg reachyMessage
		_ = ws.ReadJSON(&msg)
	}))
	defer s.Close()
	_, err := dialReachy("ws"+strings.TrimPrefix(s.URL,"http"),"reachymini",150*time.Millisecond)
	if err == nil { t.Fatal("expected timeout") }
	select { case <-ended: case <-time.After(time.Second): t.Fatal("socket leaked") }
}

// A real Pion sender behind a simulated GStreamer signalling server.
// The RTP payload is intentionally synthetic: this tests transport, not decoding.
func TestReachyRTPAndCleanup(t *testing.T) {
	api, err := reachyAPI()
	if err != nil { t.Fatal(err) }
	pc, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil { t.Fatal(err) }
	defer pc.Close()
	track, err := pion.NewTrackLocalStaticRTP(pion.RTPCodecCapability{
		MimeType:pion.MimeTypeH264,ClockRate:90000,
		SDPFmtpLine:"level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
	},"video","reachy")
	if err != nil { t.Fatal(err) }
	_, err = pc.AddTransceiverFromTrack(track,pion.RTPTransceiverInit{Direction:pion.RTPTransceiverDirectionSendonly})
	if err != nil { t.Fatal(err) }
	// Mirrors the robot's data channel, but the client does not require it.
	_, err = pc.CreateDataChannel("control",nil)
	if err != nil { t.Fatal(err) }
	offer, err := pc.CreateOffer(nil)
	if err != nil { t.Fatal(err) }
	gathered := pion.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil { t.Fatal(err) }
	select { case <-gathered: case <-time.After(5*time.Second): t.Fatal("gather timeout") }
	ended := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		defer close(ended)
		ws, e := (&websocket.Upgrader{}).Upgrade(w,r,nil)
		if e != nil { return }
		defer ws.Close()
		_ = ws.WriteJSON(map[string]any{"type":"welcome"})
		var msg reachyMessage
		if ws.ReadJSON(&msg)!=nil || msg.Type!="list" { t.Error("expected list"); return }
		_ = ws.WriteJSON(map[string]any{"type":"list","producers":[]any{
			map[string]any{"id":"robot","meta":map[string]string{"name":"reachymini"}},
		}})
		if ws.ReadJSON(&msg)!=nil || msg.Type!="startSession" || msg.PeerID!="robot" { t.Error("expected session request"); return }
		_ = ws.WriteJSON(reachyMessage{Type:"sessionStarted",SessionID:"session"})
		_ = ws.WriteJSON(reachyMessage{Type:"peer",SessionID:"session",SDP:pc.LocalDescription()})
		for {
			msg = reachyMessage{}
			if ws.ReadJSON(&msg)!=nil { return }
			if msg.SDP!=nil {
				if strings.Contains(msg.SDP.SDP,"a=candidate:") { t.Error("ICE must be trickled separately") }
				if e=pc.SetRemoteDescription(*msg.SDP);e!=nil { t.Error(e);return }
			}
			if msg.ICE!=nil {
				if e=pc.AddICECandidate(*msg.ICE);e!=nil { t.Error(e);return }
			}
		}
	}))
	defer s.Close()
	p,err := dialReachy("ws"+strings.TrimPrefix(s.URL,"http"),"reachymini",5*time.Second)
	if err!=nil { t.Fatal(err) }
	defer p.Stop()
	if len(p.GetMedias())!=1 { t.Fatalf("medias: %d",len(p.GetMedias())) }
	media:=p.GetMedias()[0]
	receiver,err:=p.GetTrack(media,media.Codecs[0])
	if err!=nil { t.Fatal(err) }
	received:=make(chan []byte,1)
	sender:=core.NewSender(media,media.Codecs[0])
	sender.Handler=func(packet *rtp.Packet) { select {case received<-append([]byte(nil),packet.Payload...):default:} }
	sender.HandleRTP(receiver)
	defer sender.Close()
	startEnded:=make(chan struct{})
	go func(){ _=p.Start();close(startEnded) }()
	payload:=[]byte{0x65,1,2,3,4,5}
	for n:=0;n<100;n++ {
		err=track.WriteRTP(&rtp.Packet{Header:rtp.Header{Version:2,SequenceNumber:uint16(n),Timestamp:uint32(n)*3000,Marker:true},Payload:payload})
		if err!=nil { t.Fatal(err) }
		select {
		case got:=<-received:
			if !bytes.Equal(got,payload) { t.Fatal("compressed payload changed") }
			_ = p.Stop()
			select {case <-ended:case <-time.After(time.Second):t.Fatal("signalling socket leaked")}
			select {case <-startEnded:case <-time.After(time.Second):t.Fatal("Start leaked")}
			return
		case <-time.After(20*time.Millisecond):
		}
	}
	t.Fatal("no RTP forwarded")
}

func TestReachyCandidateMessage(t *testing.T) {
	var msg reachyMessage
	if err:=json.Unmarshal([]byte(`{"type":"peer","sessionId":"s","ice":{"candidate":"candidate:1","sdpMLineIndex":1}}`),&msg);err!=nil {t.Fatal(err)}
	if msg.ICE==nil || msg.ICE.SDPMLineIndex==nil || *msg.ICE.SDPMLineIndex!=1 {t.Fatal("lost m-line index")}
}
