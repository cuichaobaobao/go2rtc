# Reachy Mini adapter (experimental)

Branch: reachy-mini. Not released or tested against physical hardware yet.

Source configuration (example only):
```yaml
streams:
  reachy:
    - "webrtc:ws://ROBOT_IP:8443#format=reachy"
```

The producer name defaults to reachymini; override with #format=reachy#producer=NAME.
The adapter implements GStreamer producer-offers signalling and forwards compressed
RTP through go2rtc. No PyAV, aiortc, FFmpeg invocation, JPEG encoder or camera
capture is added. RTSP consumers can use go2rtc's normal /reachy stream.
Audio is forwarded as compressed packets when requested, not decoded or recorded
by this adapter. No robot control commands are sent.

Connection establishment is bounded; the signalling socket stays alive until
the producer stops. WebSocket pings detect dead signalling peers. go2rtc owns
on-demand source start/stop. Inactive readers and real GStreamer compatibility
still require deployment testing.

Tests use a simulated signalling server and a real Pion peer. Synthetic H264 RTP
payload equality verifies compressed transport only, not picture quality or a
complete Frigate/RTSP/browser playback path. No private LAN device is contacted
by CI. RSA DTLS support and real robot interoperability require further tests.

Scope: this repository only. No HA dependencies or robot software changes.
No release or image publishing workflow is added in this phase.
