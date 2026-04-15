"""HLS pipeline — download, remux, playlist generation, codec probing."""

from __future__ import annotations

import json
import random
import re
import subprocess
import sys
import time
from pathlib import Path


def _extract_ytdlp_error(stderr: str) -> str:
    """Extract the most useful error line from yt-dlp stderr."""
    if not stderr:
        return "(no error output)"
    for line in reversed(stderr.strip().splitlines()):
        line = line.strip()
        if line.startswith("ERROR:"):
            return line
    last = stderr.strip().splitlines()[-1].strip()
    return last[:300] if last else "(no error output)"


class HlsPipeline:
    """Manages the HLS encoding pipeline: download → remux → master playlist.

    Injected with consumer config dict. Handles yt-dlp downloads, ffmpeg remux,
    codec probing, and bandwidth measurement.
    """

    def __init__(self, config: dict, cookies_file: Path | None = None):
        self.ytdlp_cfg: dict = config.get("ytdlp", {})
        self.hls_cfg: dict = config.get("hls", {})
        self.consumer_cfg: dict = config.get("consumer", {})
        self.cookies_file = cookies_file

    # ── Format selector ──────────────────────────────────────────────────

    @staticmethod
    def build_format_selector(tier: dict) -> str:
        """Build a yt-dlp format selector string for a specific quality tier."""
        h = tier["height"]
        return (
            f"bv[height<={h}][ext=mp4][vcodec~='^(avc|h264)']+ba[ext=m4a]"
            f"/b[height<={h}][ext=mp4][vcodec~='^(avc|h264)']"
        )

    # ── ffmpeg command builder ───────────────────────────────────────────

    @staticmethod
    def build_ffmpeg_remux_cmd(
        input_path: Path,
        output_dir: Path,
        segment_duration: int = 6,
    ) -> list[str]:
        """Build ffmpeg command to remux an MP4 into HLS fMP4 segments."""
        playlist_path = str(output_dir / "playlist.m3u8")
        segment_pattern = str(output_dir / "seg_%03d.m4s")

        return [
            "ffmpeg",
            "-i",
            str(input_path),
            "-c",
            "copy",
            "-f",
            "hls",
            "-hls_time",
            str(segment_duration),
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
            "-hls_segment_filename",
            segment_pattern,
            "-hls_playlist_type",
            "vod",
            "-hls_flags",
            "independent_segments",
            "-hls_list_size",
            "0",
            playlist_path,
        ]

    # ── Master playlist ──────────────────────────────────────────────────

    @staticmethod
    def generate_master_playlist(completed_tiers: list[dict]) -> str:
        """Generate a multi-variant HLS master playlist from completed tier metadata.

        Raises ValueError if no tiers provided.
        """
        if not completed_tiers:
            raise ValueError("No tiers available — minimum 1 tier required to generate master playlist")

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            "#EXT-X-INDEPENDENT-SEGMENTS",
        ]

        for tier in completed_tiers:
            lines.append(
                f'#EXT-X-STREAM-INF:BANDWIDTH={tier["bandwidth"]},RESOLUTION={tier["resolution"]},CODECS="{tier["codecs"]}"'
            )
            lines.append(f"{tier['label']}/playlist.m3u8")

        lines.append("")
        return "\n".join(lines)

    # ── Codec probing ────────────────────────────────────────────────────

    @staticmethod
    def ffprobe_streams(mp4_path: Path) -> dict | None:
        """Run ffprobe on an MP4 and return parsed stream info."""
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_streams",
            str(mp4_path),
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                return None
            data = json.loads(result.stdout)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
            return None

        streams = data.get("streams", [])
        video = None
        audio = None
        for s in streams:
            if s.get("codec_type") == "video" and video is None:
                video = s
            elif s.get("codec_type") == "audio" and audio is None:
                audio = s
        return {"video": video, "audio": audio}

    @staticmethod
    def build_codec_string(probe: dict) -> str:
        """Build RFC 6381 codec string from ffprobe stream data."""
        parts = []

        video = probe.get("video")
        if video and video.get("codec_name") in ("h264", "avc1"):
            profile = video.get("profile", "").lower()
            level = video.get("level", 31)

            profile_map = {
                "baseline": "42",
                "constrained baseline": "42",
                "main": "4d",
                "high": "64",
                "high 10": "6e",
                "high 4:2:2": "7a",
                "high 4:4:4 predictive": "f4",
            }
            profile_hex = profile_map.get(profile, "64")
            constraint_hex = "c0" if "constrained" in profile else "00"
            level_hex = f"{level:02x}"
            parts.append(f"avc1.{profile_hex}{constraint_hex}{level_hex}")
        else:
            parts.append("avc1.640028")

        audio = probe.get("audio")
        if audio and audio.get("codec_name") == "aac":
            aot_map = {"lc": "2", "he-aac": "5", "he-aacv2": "29"}
            audio_profile = (audio.get("profile") or "lc").lower().replace("_", "-")
            aot = aot_map.get(audio_profile, "2")
            parts.append(f"mp4a.40.{aot}")
        else:
            parts.append("mp4a.40.2")

        return ",".join(parts)

    # ── Bandwidth measurement ────────────────────────────────────────────

    @staticmethod
    def measure_peak_bandwidth(hls_dir: Path, target_duration: float) -> int | None:
        """Measure peak segment bitrate per RFC 8216 sliding-window method."""
        playlist = hls_dir / "playlist.m3u8"
        if not playlist.exists():
            return None

        segments: list[tuple[float, int]] = []
        lines = playlist.read_text(encoding="utf-8").splitlines()
        for i, line in enumerate(lines):
            if line.startswith("#EXTINF:"):
                duration = float(line.split(":")[1].rstrip(","))
                if i + 1 < len(lines):
                    seg_file = hls_dir / lines[i + 1].strip()
                    if seg_file.exists():
                        segments.append((duration, seg_file.stat().st_size))

        if not segments:
            return None

        init_file = hls_dir / "init.mp4"
        init_size = init_file.stat().st_size if init_file.exists() else 0

        min_dur = target_duration * 0.5
        max_dur = target_duration * 1.5
        peak = 0

        for start in range(len(segments)):
            total_bytes = init_size
            total_dur = 0.0
            for end in range(start, len(segments)):
                total_dur += segments[end][0]
                total_bytes += segments[end][1]
                if total_dur > max_dur:
                    break
                if total_dur >= min_dur:
                    bitrate = int((total_bytes * 8) / total_dur)
                    if bitrate > peak:
                        peak = bitrate

        return peak if peak > 0 else None

    # ── info.json parsing ────────────────────────────────────────────────

    @staticmethod
    def parse_info_json(info_path: Path) -> dict | None:
        """Extract video metadata fields from yt-dlp's .info.json file."""
        try:
            data = json.loads(info_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as e:
            print(f"  Warning: failed to parse {info_path}: {e}")
            return None

        chapters = data.get("chapters")
        if chapters and isinstance(chapters, list):
            chapters = [
                {
                    "title": ch.get("title", ""),
                    "start_time": ch.get("start_time", 0),
                    "end_time": ch.get("end_time", 0),
                }
                for ch in chapters
            ]
        else:
            chapters = None

        upload_date = data.get("upload_date")
        published_at = None
        if upload_date and len(upload_date) == 8:
            try:
                published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00Z"
            except (ValueError, IndexError):
                pass

        return {
            "title": data.get("title") or data.get("fulltitle") or "Untitled",
            "description": data.get("description") or "",
            "duration_seconds": data.get("duration"),
            "view_count": data.get("view_count"),
            "like_count": data.get("like_count"),
            "comment_count": data.get("comment_count"),
            "published_at": published_at,
            "thumbnail_url": data.get("thumbnail") or "",
            "handle": data.get("uploader_id") or "",
            "tags": data.get("tags") or [],
            "categories": data.get("categories") or [],
            "chapters": json.dumps(chapters) if chapters else None,
            "width": data.get("width"),
            "height": data.get("height"),
            "fps": data.get("fps"),
            "language": data.get("language"),
            "webpage_url": data.get("webpage_url") or "",
        }

    # ── Tier metadata extraction ─────────────────────────────────────────

    def extract_tier_metadata(self, tier: dict) -> dict:
        """Extract bandwidth, resolution, and codecs for a tier."""
        bandwidth = tier.get("bandwidth", 2500000)
        height = tier.get("height", 720)
        width = int(height * 16 / 9)
        codecs = "avc1.640028,mp4a.40.2"

        mp4_path = tier.get("mp4_path")
        if mp4_path and Path(mp4_path).exists():
            probe = self.ffprobe_streams(Path(mp4_path))
            if probe:
                codecs = self.build_codec_string(probe)
                video = probe.get("video")
                if video:
                    actual_w = video.get("width")
                    actual_h = video.get("height")
                    if actual_w and actual_h:
                        width = actual_w
                        height = actual_h

        hls_dir = tier.get("hls_dir")
        if hls_dir:
            peak = self.measure_peak_bandwidth(Path(hls_dir), 6)
            if peak:
                bandwidth = peak

        resolution = f"{width}x{height}"
        return {"bandwidth": bandwidth, "resolution": resolution, "codecs": codecs}

    # ── Download tiers ───────────────────────────────────────────────────

    def download_video_tier(
        self,
        video_id: str,
        staging_dir: Path,
        tier: dict,
        with_sidecars: bool = False,
    ) -> tuple[bool, Path | None, str]:
        """Download a single quality tier via yt-dlp.

        Returns (success, mp4_path_or_none, stderr).
        """
        fmt = self.build_format_selector(tier)
        output_template = str(staging_dir / f"{video_id}.%(ext)s")

        cmd = [
            sys.executable,
            "-m",
            "yt_dlp",
            "--format",
            fmt,
            "--merge-output-format",
            self.ytdlp_cfg.get("merge_output_format", "mp4"),
            "--output",
            output_template,
            "--no-playlist",
            "--no-overwrites",
        ]

        if self.cookies_file and self.cookies_file.exists():
            cmd.extend(["--cookies", str(self.cookies_file)])

        if self.ytdlp_cfg.get("match_filters"):
            cmd.extend(["--match-filters", self.ytdlp_cfg["match_filters"]])

        rc = self.ytdlp_cfg.get("remote_components")
        if rc:
            components = rc if isinstance(rc, str) else "ejs:github,ejs:npm"
            for comp in components.split(","):
                cmd.extend(["--remote-components", comp.strip()])

        if with_sidecars:
            if self.ytdlp_cfg.get("write_thumbnail"):
                cmd.append("--write-thumbnail")
            if self.ytdlp_cfg.get("write_subs"):
                cmd.append("--write-subs")
            if self.ytdlp_cfg.get("write_auto_subs"):
                cmd.append("--write-auto-subs")
            if self.ytdlp_cfg.get("sub_langs"):
                cmd.extend(["--sub-langs", self.ytdlp_cfg["sub_langs"]])
            if self.ytdlp_cfg.get("sub_format"):
                cmd.extend(["--sub-format", self.ytdlp_cfg["sub_format"]])
            if self.ytdlp_cfg.get("write_info_json"):
                cmd.append("--write-info-json")
            if self.ytdlp_cfg.get("sleep_interval_subtitles"):
                cmd.extend(["--sleep-subtitles", str(self.ytdlp_cfg["sleep_interval_subtitles"])])

        cmd.append(f"https://www.youtube.com/watch?v={video_id}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

        if result.returncode != 0:
            return (False, None, result.stderr)

        mp4_path = None
        for p in staging_dir.iterdir():
            if p.name.startswith(video_id) and p.suffix.lower() == ".mp4":
                mp4_path = p
                break

        return (True, mp4_path, result.stderr)

    def download_video_tiers(
        self,
        video_id: str,
        staging_dir: Path,
        verbose: bool = False,
    ) -> tuple[list[dict], dict[str, Path]]:
        """Download multiple quality tiers for a video.

        Returns (completed_tiers_with_mp4_path, sidecar_files).
        """
        tiers = self.hls_cfg.get(
            "tiers",
            [
                {"label": "360p", "height": 360, "bandwidth": 800000},
                {"label": "480p", "height": 480, "bandwidth": 1200000},
                {"label": "720p", "height": 720, "bandwidth": 2500000},
                {"label": "1080p", "height": 1080, "bandwidth": 5000000},
            ],
        )

        throttle_min = self.consumer_cfg.get("throttle_min_seconds", 2)
        throttle_max = self.consumer_cfg.get("throttle_max_seconds", 5)

        completed_tiers = []
        sidecar_files: dict[str, Path] = {}
        tier_errors: list[str] = []

        for i, tier in enumerate(tiers):
            label = tier["label"]
            tier_dir = staging_dir / label
            tier_dir.mkdir(parents=True, exist_ok=True)

            is_last = i == len(tiers) - 1

            print(f"    Downloading {video_id} {label}...", end="", flush=True)

            start_time = time.time()
            success, mp4_path, stderr = self.download_video_tier(
                video_id,
                tier_dir,
                tier,
                with_sidecars=is_last,
            )
            elapsed = time.time() - start_time

            if success and mp4_path:
                tier_result = {**tier, "mp4_path": mp4_path}
                completed_tiers.append(tier_result)
                size_mb = mp4_path.stat().st_size / (1024 * 1024)
                print(f" OK ({size_mb:.1f} MB, {elapsed:.0f}s)")

                if is_last:
                    for p in tier_dir.iterdir():
                        if not p.name.startswith(video_id):
                            continue
                        suffix = p.suffix.lower()
                        name = p.name
                        if suffix in (".jpg", ".jpeg", ".webp", ".png"):
                            sidecar_files["thumbnail"] = p
                        elif suffix == ".vtt" or name.endswith(".vtt"):
                            sidecar_files["subtitle"] = p
                        elif name.endswith(".info.json"):
                            sidecar_files["info_json"] = p
            else:
                error_line = _extract_ytdlp_error(stderr)
                print(f" FAILED ({elapsed:.0f}s)")
                print(f"      {error_line}")
                tier_errors.append(f"{label}: {error_line}")
                if verbose and len(stderr) > len(error_line):
                    print(f"      Full stderr: {stderr[:500]}")

            if i < len(tiers) - 1 and throttle_max > 0:
                if not success:
                    delay = random.uniform(throttle_min * 2, throttle_max * 2)
                else:
                    delay = random.uniform(throttle_min, throttle_max)
                if verbose:
                    print(f"      Throttle: {delay:.1f}s")
                time.sleep(delay)

        return completed_tiers, sidecar_files, tier_errors

    def remux_to_hls(
        self,
        completed_tiers: list[dict],
        staging_dir: Path,
        verbose: bool = False,
    ) -> list[dict]:
        """Remux each downloaded tier MP4 into HLS fMP4 segments."""
        segment_duration = self.hls_cfg.get("segment_duration", 6)
        remuxed_tiers = []

        for tier in completed_tiers:
            label = tier["label"]
            mp4_path = tier["mp4_path"]
            hls_dir = staging_dir / "hls" / label
            hls_dir.mkdir(parents=True, exist_ok=True)

            cmd = self.build_ffmpeg_remux_cmd(mp4_path, hls_dir, segment_duration)

            print(f"    Remuxing {label}...", end="", flush=True)

            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, cwd=hls_dir)
            elapsed = time.time() - start_time

            if result.returncode != 0:
                print(f" FAILED ({elapsed:.0f}s)")
                if verbose:
                    print(f"      {result.stderr[:200]}")
                continue

            playlist = hls_dir / "playlist.m3u8"
            if not playlist.exists():
                print(f" FAILED (no playlist)")
                continue

            tier_result = {**tier, "hls_dir": hls_dir}
            remuxed_tiers.append(tier_result)

            seg_count = len(list(hls_dir.glob("seg_*.m4s")))
            print(f" OK ({seg_count} segments, {elapsed:.1f}s)")

        # Align EXT-X-TARGETDURATION across all tiers
        if len(remuxed_tiers) > 1:
            max_target = 0
            for tier in remuxed_tiers:
                playlist = tier["hls_dir"] / "playlist.m3u8"
                for line in playlist.read_text(encoding="utf-8").splitlines():
                    if line.startswith("#EXT-X-TARGETDURATION:"):
                        val = int(line.split(":")[1])
                        if val > max_target:
                            max_target = val
                        break

            if max_target > 0:
                for tier in remuxed_tiers:
                    playlist = tier["hls_dir"] / "playlist.m3u8"
                    content = playlist.read_text(encoding="utf-8")
                    content = re.sub(
                        r"#EXT-X-TARGETDURATION:\d+",
                        f"#EXT-X-TARGETDURATION:{max_target}",
                        content,
                    )
                    playlist.write_text(content, encoding="utf-8")

                if verbose:
                    print(f"    Aligned EXT-X-TARGETDURATION to {max_target}s across {len(remuxed_tiers)} tiers")

        return remuxed_tiers
