"""Boot the built demo in a headless browser and check it the way a visitor would use it.

With every fault switched on at 60x, no invariant may break and the page may log no errors; then,
with a bug planted, the invariants must catch it.

    python scripts/demo_smoke.py --site _site [--channel chrome] [--screenshot demo.png]
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import http.server
import threading
from pathlib import Path
from typing import Any

from playwright.async_api import Page, async_playwright


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass


def serve(site: Path) -> http.server.ThreadingHTTPServer:
    handler = functools.partial(Quiet, directory=str(site))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def click(page: Page, selector: str) -> None:
    await page.evaluate("(s) => document.querySelector(s).click()", selector)


async def broken(page: Page) -> list[str]:
    return [str(await li.get_attribute("data-inv")) for li in await page.query_selector_all("#invariants li.bad")]


async def run(site: Path, channel: str | None, screenshot: Path | None) -> None:
    server = serve(site)
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    errors: list[str] = []
    async with async_playwright() as p:
        launch: dict[str, Any] = {"args": ["--no-sandbox"]}
        if channel:
            launch["channel"] = channel
        browser = await p.chromium.launch(**launch)
        page = await browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        await page.goto(url)
        await page.wait_for_selector("#app:not([hidden])", timeout=120_000)
        print("started:", await page.text_content("#runtime"))

        for switch in ("chaos", "crashing", "lying"):
            await click(page, f"[data-switch={switch}]")
        await click(page, '#speed button[data-speed="60"]')
        await click(page, '.fault[data-psp="alpha"][data-fault="slow"]')
        await page.wait_for_timeout(12_000)
        await click(page, '.fault[data-psp="charlie"][data-fault="down"]')
        await click(page, "#crash")
        await page.wait_for_timeout(12_000)
        kpis = await page.text_content("#kpis")
        print("after every fault:", await page.text_content("#clock"), "|", await page.text_content("#perf"))
        assert not await broken(page), f"an invariant broke with no bug planted: {await broken(page)}"
        assert "Crashes survived" in str(kpis)

        await page.select_option("#bug", "resend_on_timeout")
        for _ in range(60):
            await page.wait_for_timeout(500)
            if await broken(page):
                break
        caught = await broken(page)
        print("planted resend_on_timeout, caught by:", caught)
        assert caught, "a planted bug went unnoticed"

        if screenshot:
            await click(page, "#reset")
            await page.wait_for_timeout(9_000)
            await page.screenshot(path=str(screenshot))
        await browser.close()
    server.shutdown()
    assert not errors, errors
    print("ok")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", type=Path, default=Path("_site"))
    ap.add_argument("--channel", help="a browser channel such as chrome; default: Playwright's Chromium")
    ap.add_argument("--screenshot", type=Path)
    args = ap.parse_args()
    asyncio.run(run(args.site, args.channel, args.screenshot))


if __name__ == "__main__":
    main()
