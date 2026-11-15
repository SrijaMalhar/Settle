"""Render docs/demo.gif: the real output of scripts/demo.py, drawn as a terminal session.

    pip install pillow
    python scripts/demo_gif.py docs/demo.gif

The demo runs first and its output is captured, so the GIF always shows what the code prints.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
SIZE, LINE, PAD, BAR = 15, 21, 22, 34

BG, BAR_BG, TEXT, DIM = "#0d1117", "#161b22", "#c9d1d9", "#7d8590"
BLUE, GREEN, YELLOW, ORANGE, RED, WHITE = "#79c0ff", "#7ee787", "#e3b341", "#ffa657", "#ff7b72", "#f0f6fc"
KIND = {
    "PaymentPaid": GREEN, "RefundConfirmed": GREEN, "PaymentExpired": YELLOW, "EscalatedToOps": ORANGE,
    "RefundStarted": ORANGE, "RefundRetried": ORANGE, "(second tap)": RED,
}
WORDS = {"-> succeeded": GREEN, "-> pending": YELLOW, "-> unresolved": ORANGE, "still pending": YELLOW,
         "refused:": RED, "money returned": GREEN, "waiting for a person": ORANGE}


def segments(line: str) -> list[tuple[str, str, bool]]:
    """Split one output line into (text, colour, bold) runs."""
    if line.startswith("$ "):
        return [("$ ", GREEN, True), (line[2:], WHITE, False)]
    if line[:1].isdigit():
        return [(line, WHITE, True)]
    if line.startswith("  =>"):
        return [(line, GREEN, True)]
    if not line.startswith("  +"):
        return [(line, TEXT, False)]
    stamp, kind, rest = line[:8], line[8:25], line[25:]
    out = [(stamp, DIM, False), (kind, KIND.get(kind.strip(), BLUE), False)]
    while rest:
        hits = [(rest.find(w), w) for w in WORDS if w in rest]
        if not hits:
            out.append((rest, TEXT, False))
            break
        at, word = min(hits)
        out += [(rest[:at], TEXT, False), (word, WORDS[word], False)]
        rest = rest[at + len(word):]
    return out


def frame(lines: list[str], width: int, height: int, fonts: dict[bool, ImageFont.FreeTypeFont],
          cursor: bool) -> Image.Image:
    img = Image.new("RGB", (width, height), BG)
    d = ImageDraw.Draw(img)
    d.rectangle((0, 0, width, BAR), fill=BAR_BG)
    for i, colour in enumerate(("#ff5f57", "#febc2e", "#28c840")):
        d.ellipse((16 + i * 22, 11, 28 + i * 22, 23), fill=colour)
    title = "settle: python scripts/demo.py"
    d.text(((width - fonts[False].getlength(title)) / 2, 9), title, font=fonts[False], fill=DIM)
    y = BAR + PAD - 4
    for line in lines:
        x = float(PAD)
        for text, colour, bold in segments(line):
            d.text((x, y), text, font=fonts[bold], fill=colour)
            x += fonts[bold].getlength(text)
        y += LINE
    if cursor:
        d.rectangle((PAD, y + 3, PAD + 9, y + 17), fill=TEXT)
    return img


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out", nargs="?", default=str(ROOT / "docs" / "demo.gif"))
    args = ap.parse_args()
    output = subprocess.run([sys.executable, str(ROOT / "scripts" / "demo.py")], capture_output=True, text=True,
                            check=True).stdout
    lines = ["$ python scripts/demo.py", *output.splitlines()]
    fonts = {False: ImageFont.truetype(FONT, SIZE), True: ImageFont.truetype(BOLD, SIZE)}
    cols = max(len(line) for line in lines)
    width = int(PAD * 2 + cols * fonts[False].getlength("M")) + 4
    height = BAR + PAD * 2 + LINE * (len(lines) + 1)

    frames: list[Image.Image] = [frame(lines[:1], width, height, fonts, True)]
    durations = [900]
    for n in range(2, len(lines) + 1):
        frames.append(frame(lines[:n], width, height, fonts, n == len(lines)))
        line = lines[n - 1]
        durations.append(1100 if line.startswith("  =>") else 450 if line[:1].isdigit() or not line else 130)
    durations[-1] = 5000

    # One palette for every frame, so colours never flicker between frames.
    palette = frames[-1].quantize(colors=96, method=Image.Quantize.MEDIANCUT)
    quantised = [f.quantize(palette=palette, dither=Image.Dither.NONE) for f in frames]
    quantised[0].save(args.out, save_all=True, append_images=quantised[1:], duration=durations, loop=0,
                      optimize=True, disposal=1)
    size = Path(args.out).stat().st_size
    print(f"{args.out}: {width}x{height}, {len(frames)} frames, {sum(durations) / 1000:.1f} s, {size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
