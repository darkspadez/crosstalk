# Probe fixtures

`corpus.json` is the probe's recorded evidence: what the **real** ffprobe and ffmpeg printed
when the probe's exact command lines were run against synthetic media served from a throwaway
local HTTP server. `scripts/probe_fixtures.py` generates both the media and the file — nothing
here was written by hand, and nothing here came from a provider.

Two things worth knowing before editing it:

- **the media is not committed.** ffmpeg's `lavfi` sources make it in a temp directory when the
  corpus is regenerated, so this directory stays text. `python3 -I scripts/probe_fixtures.py`
  rebuilds it (needs ffprobe and ffmpeg, opens no network).
- **`stdout` is a projection of ffprobe's JSON.** A full `-show_packets` document for a few
  seconds of video is hundreds of kilobytes; the projection keeps every field the parser reads
  (`streams[].codec_type/codec_name/width/height/pix_fmt/r_frame_rate/bit_rate/sample_rate/
  channels/channel_layout/index`, `format.format_name/duration/bit_rate`, each packet's
  `stream_index/size/duration_time`) and every value in it is what the tool really printed.
  `--full` keeps the documents whole for a one-off look.

What the corpus covers, because these are the places a port drifts:

| case | what it pins down |
|---|---|
| `alive_hd`, `alive_sd`, `alive_uhd`, `alive_slow` | the media facts, the format ladder, and a live TS with **no** bitrate anywhere (so the 30-packet fallback supplies one) |
| `hls_live` | an HLS feed: a container bitrate in bps (0.228 kbps, which rounds to 0), `hls` as the stream type |
| `radio` | exit 0 with no video stream: `No Video Stream`, which the plugin treats as **Skipped** |
| `alive_hd_content` | one clean ffmpeg pass with all three detectors armed: no verdict, `mean_volume_db` recorded |
| `black` | blackdetect and freezedetect both fire; black must win, and the evidence block must NOT carry the volume |
| `frozen` | a still, non-black picture: only freezedetect fires |
| `silent` | `mean_volume: -91.0 dB`: a silent verdict *does* carry the level it quotes |
| `placeholder`, `placeholder_off` | a finite MP4 on a live URL: the container duration is the fingerprint, and the evidence is recorded even when the detector is off |
| `not_found`, `forbidden`, `rate_limited`, `server_error`, `invalid_data`, `refused`, `timeout` | the classifier: 404/403/429, a real 500 body (`5XX Server Error reply`, which is *not* `Server Error`), invalid data, a refused connection, and ffprobe's own read timeout |

`scripts/probe_parity.py` replays all of it through IPTV Checker's own `check_stream` and through
`crates/streams/examples/probe_io`. It also constructs the two cases no tool can record: a
process timeout (`subprocess.TimeoutExpired`) and a Streamlink-only host, where no subprocess
runs at all.

Regenerating changes the file's `tools` line as well as any output the tools changed — that diff
is the point: it is how an upstream ffmpeg bump that breaks the parse shows up.
