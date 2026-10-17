package webrtc

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	rtc "github.com/AlexxIT/go2rtc/pkg/webrtc"
	"github.com/gorilla/websocket"
	"github.com/pion/dtls/v3"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/sdp/v3"
	pion "github.com/pion/webrtc/v4"
)

// Reachy uses GStreamer's producer-offers protocol, not go2rtc's WS protocol.
// This producer handles signalling only: media remains compressed RTP.
type reachyProducer struct {
	core.Connection
	pc            *pion.PeerConnection
	ws            *websocket.Conn
	writeMu       sync.Mutex
	mu            sync.Mutex
	closing       bool
	workers       sync.WaitGroup
	once          sync.Once
	startOnce     sync.Once
	done          chan struct{}
	closed        chan struct{}
	signalsDone   chan struct{}
	heartbeatDone chan struct{}
	stopOnce      sync.Once
	started       chan struct{}
	ready         chan struct{}
	err           error
	session       string // owned by the signalling reader
}

type reachyMessage struct {
	Type      string `json:"type"`
	PeerID    string `json:"peerId,omitempty"`
	SessionID string `json:"sessionId,omitempty"`
	Producers []struct {
		ID   string `json:"id"`
		Meta struct {
			Name string `json:"name"`
		} `json:"meta"`
	} `json:"producers,omitempty"`
	SDP *pion.SessionDescription `json:"sdp,omitempty"`
	ICE *pion.ICECandidateInit   `json:"ice,omitempty"`
}

func reachyAPI() (*pion.API, error) {
	m := &pion.MediaEngine{}
	if err := rtc.RegisterDefaultCodecs(m); err != nil {
		return nil, err
	}
	i := &interceptor.Registry{}
	if err := pion.RegisterDefaultInterceptors(m, i); err != nil {
		return nil, err
	}
	s := pion.SettingEngine{}
	// Reachy's GStreamer server may use an RSA certificate.
	// Fingerprint verification remains enabled.
	s.SetDTLSCipherSuites(
		dtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		dtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	)
	return pion.NewAPI(pion.WithMediaEngine(m), pion.WithInterceptorRegistry(i), pion.WithSettingEngine(s)), nil
}

func reachyClient(rawURL string, query url.Values) (core.Producer, error) {
	name := query.Get("producer")
	if name == "" {
		name = "reachymini"
	}
	return dialReachy(rawURL, name, 20*time.Second)
}

func dialReachy(rawURL, name string, timeout time.Duration) (*reachyProducer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, errors.New("reachy: expected ws or wss")
	}
	dialer := websocket.Dialer{HandshakeTimeout: timeout}
	ws, resp, err := dialer.Dial(rawURL, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	ws.SetReadLimit(1 << 20)
	api, err := reachyAPI()
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	pc, err := api.NewPeerConnection(pion.Configuration{})
	if err != nil {
		_ = ws.Close()
		return nil, err
	}
	p := &reachyProducer{
		Connection: core.Connection{ID: core.NewID(), FormatName: "webrtc/reachy", Protocol: "ws", URL: rawURL},
		pc:         pc, ws: ws, done: make(chan struct{}), closed: make(chan struct{}),
		started: make(chan struct{}), ready: make(chan struct{}),
		signalsDone: make(chan struct{}), heartbeatDone: make(chan struct{}),
	}
	var readyOnce sync.Once
	pc.OnConnectionStateChange(func(state pion.PeerConnectionState) {
		switch state {
		case pion.PeerConnectionStateConnected:
			readyOnce.Do(func() { close(p.ready) })
		case pion.PeerConnectionStateFailed, pion.PeerConnectionStateDisconnected, pion.PeerConnectionStateClosed:
			p.finish(fmt.Errorf("reachy: peer connection %s", state))
		}
	})
	pc.OnTrack(func(track *pion.TrackRemote, receiver *pion.RTPReceiver) {
		p.mu.Lock()
		if p.closing {
			p.mu.Unlock()
			return
		}
		p.workers.Add(1)
		p.mu.Unlock()
		defer p.workers.Done()
		select {
		case <-p.started:
		case <-p.done:
			return
		}
		var target *core.Receiver
		for _, tr := range pc.GetTransceivers() {
			if tr.Receiver() != receiver {
				continue
			}
			for _, r := range p.Receivers {
				if r.Media.ID == tr.Mid() && r.Codec.PayloadType == uint8(track.PayloadType()) {
					target = r
					break
				}
			}
		}
		if target == nil {
			return
		}
		// Request a keyframe for new readers; no video decoding takes place here.
		if track.Kind() == pion.RTPCodecTypeVideo {
			_ = pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
		}
		for {
			packet, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			target.WriteRTP(packet)
		}
	})
	go p.readSignals(name, timeout)
	go p.heartbeat()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.ready:
		select {
		case <-p.done:
			<-p.closed
			return nil, p.err
		default:
		}
		return p, nil
	case <-p.done:
		<-p.closed
		return nil, p.err
	case <-timer.C:
		p.finish(errors.New("reachy: connection timeout"))
		<-p.closed
		return nil, p.err
	}
}

func (p *reachyProducer) write(msg reachyMessage) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if err := p.ws.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return err
	}
	return p.ws.WriteJSON(msg)
}

func (p *reachyProducer) finish(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.closing = true
		p.err = err
		p.mu.Unlock()
		close(p.done)
		_ = p.ws.Close()
		// Pion callbacks must not synchronously wait for PeerConnection.Close.
		go func() {
			_ = p.pc.Close()
			p.workers.Wait()
			<-p.signalsDone
			<-p.heartbeatDone
			close(p.closed)
		}()
	})
}

func (p *reachyProducer) heartbeat() {
	defer close(p.heartbeatDone)
	t := time.NewTicker(25 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			if err := p.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(3*time.Second)); err != nil {
				p.finish(err)
				return
			}
		}
	}
}

func (p *reachyProducer) readSignals(name string, timeout time.Duration) {
	defer close(p.signalsDone)
	var result error
	defer func() {
		if result == nil {
			result = errors.New("reachy: signalling ended")
		}
		p.finish(result)
	}()
	_ = p.ws.SetReadDeadline(time.Now().Add(timeout))
	p.ws.SetPongHandler(func(string) error { return p.ws.SetReadDeadline(time.Now().Add(75 * time.Second)) })
	var offered bool
	var pending []pion.ICECandidateInit
	for {
		var msg reachyMessage
		if result = p.ws.ReadJSON(&msg); result != nil {
			return
		}
		switch msg.Type {
		case "welcome":
			result = p.write(reachyMessage{Type: "list"})
		case "list":
			if p.session != "" || offered {
				result = errors.New("reachy: unexpected producer list")
				return
			}
			var id string
			for _, producer := range msg.Producers {
				if producer.Meta.Name == name {
					id = producer.ID
					break
				}
			}
			if id == "" {
				result = fmt.Errorf("reachy: producer %q not found", name)
				return
			}
			result = p.write(reachyMessage{Type: "startSession", PeerID: id})
		case "sessionStarted":
			if msg.SessionID == "" || p.session != "" {
				result = errors.New("reachy: invalid session")
				return
			}
			p.session = msg.SessionID
		case "peer":
			if p.session == "" || (msg.SessionID != "" && msg.SessionID != p.session) {
				result = errors.New("reachy: wrong session")
				return
			}
			if msg.SDP != nil {
				if offered || msg.SDP.Type != pion.SDPTypeOffer {
					result = errors.New("reachy: expected one producer offer")
					return
				}
				offered = true
				result = p.answer(*msg.SDP, timeout)
				if result == nil {
					for _, candidate := range pending {
						if result = p.pc.AddICECandidate(candidate); result != nil {
							break
						}
					}
					pending = nil
				}
				if result == nil {
					result = p.ws.SetReadDeadline(time.Now().Add(75 * time.Second))
				}
			} else if msg.ICE != nil && msg.ICE.Candidate != "" {
				if !offered {
					if len(pending) >= 64 {
						result = errors.New("reachy: too many pending candidates")
						return
					}
					pending = append(pending, *msg.ICE)
				} else {
					result = p.pc.AddICECandidate(*msg.ICE)
				}
			}
		case "endSession":
			result = errors.New("reachy: producer ended session")
			return
		case "error":
			result = errors.New("reachy: signalling server error")
			return
		}
		if result != nil {
			return
		}
	}
}

func (p *reachyProducer) answer(offer pion.SessionDescription, timeout time.Duration) error {
	if err := p.pc.SetRemoteDescription(offer); err != nil {
		return err
	}
	// Answer receive-only even if the producer offers two-way audio.
	for _, tr := range p.pc.GetTransceivers() {
		// Expose a single negotiated codec so downstream readers cannot select
		// a different payload type from the producer.
		if receiver := tr.Receiver(); receiver != nil {
			codecs := receiver.GetParameters().Codecs
			var selected *pion.RTPCodecParameters
			for i := range codecs {
				codec := &codecs[i]
				if strings.EqualFold(codec.MimeType, pion.MimeTypeH264) || strings.EqualFold(codec.MimeType, pion.MimeTypeOpus) {
					if selected == nil {
						selected = codec
					}
					if strings.Contains(codec.SDPFmtpLine, "profile-level-id=42e01f") {
						selected = codec
						break
					}
				}
			}
			if selected == nil {
				return errors.New("reachy: expected H264 video or Opus audio")
			}
			if err := tr.SetCodecPreferences([]pion.RTPCodecParameters{*selected}); err != nil {
				return err
			}
		}
		if tr.Sender() != nil {
			if err := tr.SetSender(tr.Sender(), nil); err != nil {
				return err
			}
		}
	}
	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	// Prepare immutable receivers before media can arrive.
	sd := &sdp.SessionDescription{}
	if err = sd.Unmarshal([]byte(answer.SDP)); err != nil {
		return err
	}
	for _, md := range sd.MediaDescriptions {
		if md.MediaName.Port.Value == 0 {
			continue
		}
		media := core.UnmarshalMedia(md)
		if media.Direction != core.DirectionRecvonly {
			continue
		}
		media.Codecs = rtc.SkipNonMediaCodecs(media.Codecs)
		if len(media.Codecs) == 0 {
			continue
		}
		p.Medias = append(p.Medias, media)
		for _, codec := range media.Codecs {
			p.Receivers = append(p.Receivers, core.NewReceiver(media, codec))
		}
	}
	if len(p.Medias) == 0 {
		return errors.New("reachy: no compatible media")
	}
	gathered := pion.GatheringCompletePromise(p.pc)
	if err = p.pc.SetLocalDescription(answer); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-gathered:
	case <-p.done:
		return errors.New("reachy: closed during ICE gathering")
	case <-timer.C:
		return errors.New("reachy: ICE gathering timeout")
	}
	// GStreamer needs separate ICE messages, even when candidates occur in SDP.
	var lines []string
	var candidates []pion.ICECandidateInit
	index := -1
	for _, line := range strings.Split(p.pc.LocalDescription().SDP, "\r\n") {
		if strings.HasPrefix(line, "m=") {
			index++
		}
		if strings.HasPrefix(line, "a=candidate:") && index >= 0 {
			n := uint16(index)
			candidates = append(candidates, pion.ICECandidateInit{Candidate: line[2:], SDPMLineIndex: &n})
		} else if line != "a=end-of-candidates" {
			lines = append(lines, line)
		}
	}
	answer.SDP = strings.Join(lines, "\r\n")
	p.SDP = answer.SDP
	if err = p.write(reachyMessage{Type: "peer", SessionID: p.session, SDP: &answer}); err != nil {
		return err
	}
	for i := range candidates {
		if err = p.write(reachyMessage{Type: "peer", SessionID: p.session, ICE: &candidates[i]}); err != nil {
			return err
		}
	}
	return nil
}

func (p *reachyProducer) Start() error {
	p.startOnce.Do(func() { close(p.started) })
	<-p.closed
	return p.err
}

func (p *reachyProducer) Stop() error {
	p.finish(errors.New("reachy: stopped"))
	<-p.closed
	p.stopOnce.Do(func() {
		for _, receiver := range p.Receivers {
			receiver.Close()
		}
	})
	return nil
}
