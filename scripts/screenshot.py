"""Screenshot the Grafana dashboard with a headless browser.

    python scripts/screenshot.py docs/grafana.png [--minutes 20]
"""

from __future__ import annotations

import argparse

from playwright.sync_api import sync_playwright


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--url", default="http://localhost:3000")
    ap.add_argument("--minutes", type=int, default=20)
    ap.add_argument("--chrome", action="store_true", help="use the installed Google Chrome")
    args = ap.parse_args()
    url = f"{args.url}/d/settle/settle?orgId=1&from=now-{args.minutes}m&to=now&kiosk&theme=dark"
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel="chrome" if args.chrome else None)
        page = browser.new_page(viewport={"width": 1600, "height": 1750}, device_scale_factor=1)
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_timeout(8_000)  # let every panel finish its queries and draw
        page.screenshot(path=args.out, full_page=True)
        browser.close()
    print(f"saved {args.out}")


if __name__ == "__main__":
    main()
