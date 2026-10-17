# Reachy Mini adapter — experimental, not released

Development revision: reachy-0.1.0-dev. Branch: reachy-mini.
No physical robot, Home Assistant or Frigate deployment has been tested.

## Source configuration (example only)

```yaml
streams:
  reachy:
    - "webrtc:ws://ROBOT_IP:8443#format=reachy"
```

The producer name defaults to reachymini; override with #format=reachy#producer=NAME.
The adapter implements GStreamer producer-offers signalling, chooses one compatible
H264/Opus codec per track and forwards compressed RTP through go2rtc.
No PyAV, aiortc, FFmpeg invocation, JPEG encoder or camera capture is added.
RTSP consumers use go2rtc's normal stream endpoint: rtsp://BRIDGE_IP:8554/reachy.
There is no audio playback, recording, or robot control in the adapter.

Connection establishment is bounded; the signalling socket stays alive until
the producer stops. WebSocket pings detect dead signalling peers. go2rtc owns
on-demand source start/stop. Actual GStreamer compatibility and complete
RTSP/browser playback still require tests.

## Cloud checks

A simulated GStreamer signalling server and real Pion peers exercise:
- missing producer and silent-server timeout cleanup;
- ECDSA and RSA DTLS handshakes, with and without a data channel;
- separate ICE messages, one negotiated codec and identical RTP payloads;
- local stop and producer endSession, socket and Start task termination;
- repeated execution under the Go race detector;
- existing upstream WebRTC tests and whole-project compilation.

Synthetic H264 payload equality verifies compressed transport, not decoded picture
quality, sustained memory use, audio playback, or Frigate integration.
The cloud test does not contact any private LAN device.

## Container

docker/reachy.Dockerfile builds a forwarding-only non-root image without
Python or FFmpeg. CI builds it and runs -version with no network.
No image is pushed or released. Snapshot/transcoding features requiring FFmpeg
are intentionally unavailable in this image. No host deployment is performed.

Scope: this GitHub repository only; no HA dependencies or robot software changes.
