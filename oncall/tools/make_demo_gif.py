"""Builds docs/oncall/demo.gif from the REAL screenshots Playwright captured in `make oncall-e2e`.

The story (SPEC section 11): live charts -> a bad record lands in the DLQ -> alert -> fix the reference data -> replay ->
parity back in sync. Each frame is a real screenshot with a caption bar; the two steps that happen outside the app
(inserting the bad row, adding the merchant) are caption cards that show the actual command used by the e2e run.
Nothing here is synthetic UI. The idle waiting in the real run (the pipeline's 120 s merchant cache) is not shown.

  python oncall/tools/make_demo_gif.py
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
SHOTS = ROOT / "docs" / "oncall" / "screenshots"
OUT = ROOT / "docs" / "oncall" / "demo.gif"
W, H, BAR = 390, 760, 92
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"

# (screenshot, step title, caption, seconds)
STORY = [
    ("03-live.png", "1. Live", "Throughput, lag, freshness and parity update every 2 s.", 3.2),
    (None, "2. A bad record arrives", "Postgres row for a merchant the validator does not know:\n$ dlq_demo.py inject MERCH_E2E_…", 2.6),
    ("04-alerts-firing.png", "3. Alert", "Lag and DLQ growth cross their thresholds: alerts fire (push + WebSocket).", 3.2),
    ("05-alerts-acknowledged.png", "4. Acknowledge", "The on-call engineer acknowledges it; the audit log records who.", 2.4),
    ("06-dlq-list.png", "5. The record is in the DLQ", "Filter by reason: UNKNOWN MERCHANT.", 2.6),
    ("07-dlq-detail.png", "6. Inspect", "Payload and validation error.", 2.6),
    (None, "7. Fix the reference data", "Add the merchant to the reference table:\n$ dlq_demo.py fix MERCH_E2E_…", 2.6),
    ("08-viewer-no-replay.png", "8. Viewers cannot replay", "No Replay button; the API answers 403 too.", 2.6),
    ("09-replay-confirm.png", "9. Admin replays", "Confirmation sheet; the action is audited.", 2.6),
    ("10-replay-applied.png", "10. APPLIED", "Validated, re-published through the normal path, now in the lake.", 3.0),
    ("11-dlq-after-replay.png", "11. It left the queue", "Replaying again is a no-op.", 2.4),
    ("13-replay-already-applied.png", "12. ALREADY_APPLIED", "The LSN guard: a second replay writes nothing.", 3.0),
    ("12-audit.png", "13. Audited", "Both replays are in the audit log under the admin's name.", 2.6),
    ("17-live-in-sync.png", "14. Parity back in sync", "Stream drained: lag 0, source and lake counts agree (diff 0).", 3.4),
]


def font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size)


def wrap(draw: ImageDraw.ImageDraw, text: str, f: ImageFont.FreeTypeFont, width: int) -> list[str]:
    lines: list[str] = []
    for para in text.split("\n"):
        cur = ""
        for word in para.split(" "):
            test = (cur + " " + word).strip()
            if draw.textlength(test, font=f) <= width:
                cur = test
            else:
                lines.append(cur)
                cur = word
        lines.append(cur)
    return lines


def frame(shot: str | None, title: str, caption: str) -> Image.Image:
    img = Image.new("RGB", (W, H + BAR), "#0e141b")
    d = ImageDraw.Draw(img)
    if shot:
        s = Image.open(SHOTS / shot).convert("RGB")
        s = s.resize((W, int(s.height * W / s.width)))
        img.paste(s.crop((0, 0, W, min(s.height, H))), (0, 0))
    else:
        d.rectangle((0, 0, W, H), fill="#17202a")
        f = font(MONO, 15)
        y = 280
        for line in wrap(d, caption.split("\n")[-1], f, W - 48):
            d.text((24, y), line, font=f, fill="#6ea0ff")
            y += 24
    d.rectangle((0, H, W, H + BAR), fill="#0b1015")
    d.text((14, H + 8), title, font=font(BOLD, 16), fill="#ffffff")
    y = H + 32
    for line in wrap(d, caption.split("\n")[0], font(FONT, 13), W - 28):
        d.text((14, y), line, font=font(FONT, 13), fill="#b8c7d6")
        y += 18
    return img.quantize(colors=96, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)


def main() -> None:
    frames, durations = [], []
    for shot, title, caption, secs in STORY:
        if shot and not (SHOTS / shot).exists():
            raise SystemExit(f"missing screenshot {shot}: run `make oncall-e2e` first")
        frames.append(frame(shot, title, caption))
        durations.append(int(secs * 1000))
    frames[0].save(OUT, save_all=True, append_images=frames[1:], duration=durations, loop=0, optimize=True, disposal=2)
    print(f"wrote {OUT} ({OUT.stat().st_size / 1024:.0f} KiB, {len(frames)} frames, {sum(durations) / 1000:.0f} s)")


if __name__ == "__main__":
    main()
