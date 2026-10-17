# Reachy Mini adapter — experimental release candidate

Image version: 0.1.0-rc.2. Branch: reachy-mini.
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
on-demand source start/stop. Actual robot GStreamer compatibility, browser playback and Frigate integration
still require tests.

## Cloud checks

A simulated GStreamer signalling server and real Pion peers exercise:
- missing producer and silent-server timeout cleanup;
- ECDSA and RSA DTLS handshakes, with and without a data channel;
- separate ICE messages, one negotiated codec and identical RTP payloads;
- local stop and producer endSession, socket and Start task termination;
- repeated execution under the Go race detector;
- existing upstream WebRTC tests and whole-project compilation.

Additional cloud media tests forward a valid 1280x720 H264 fixture through WebRTC
and the go2rtc RTSP server to two independent FFmpeg decoders for three minutes.
All 1800 frames per viewer match the source decoded pixel hash. The fixture is a
repeated static frame, not real robot footage. Resource samples cover the Go test
process (including simulated sender), not a deployed container or decoder processes.
Thirty session cycles check return of file descriptors and goroutines to baseline
without forced GC. This is a short stress check, not a long-term leak guarantee.
Audio playback and Frigate integration remain untested.
The cloud test does not contact any private LAN device.

## Container

docker/reachy.Dockerfile builds a forwarding-only non-root image without
Python or FFmpeg. CI builds it and runs -version with no network.
The release workflow publishes ghcr.io/cuichaobaobao/reachy-go2rtc:0.1.0-rc.2
for linux/amd64 and linux/arm64 after checks pass. Release tags must not be
overwritten; increment docker/REACHY_VERSION for the next publication.
Only amd64 gets a runtime smoke test; arm64 is cross-compiled.
A new GHCR package may require its owner to enable public visibility for anonymous
downloads. Publication is explicit via [publish-reachy] in a commit message or
the workflow publish input. No latest tag is published. Snapshot/transcoding features requiring FFmpeg
are intentionally unavailable in this image. No host deployment is performed.

Scope: this GitHub repository only; no HA dependencies or robot software changes.

RC2: match SDP codec MIME names case-insensitively. The real robot advertises
uppercase OPUS; the regression test includes audio first and H264 profile 42c01f.
