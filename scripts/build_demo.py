"""Build the live demo as a static site: the page, the repository's Python packages as one zip,
and the Pyodide runtime next to them, so nothing is fetched from a third party.

    npm install pyodide@0.29.5
    python scripts/build_demo.py --pyodide node_modules/pyodide --out _site
    python -m http.server -d _site 8080
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = ["index.html", "style.css", "app.js", "worker.js"]
RUNTIME = ["pyodide.js", "pyodide.asm.js", "pyodide.asm.wasm", "python_stdlib.zip", "pyodide-lock.json"]


def build(pyodide: Path, out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    (out / "pyodide").mkdir(parents=True)
    for name in PAGE:
        shutil.copy2(ROOT / "demo" / name, out / name)
    for name in RUNTIME:
        shutil.copy2(pyodide / name, out / "pyodide" / name)
    files = sorted(p for pkg in ("settle", "sim") for p in (ROOT / pkg).rglob("*.py") if "__pycache__" not in p.parts)
    files.append(ROOT / "demo" / "stubs.py")
    with zipfile.ZipFile(out / "settle.zip", "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, f.relative_to(ROOT).as_posix())
    (out / ".nojekyll").touch()
    print(f"{out}: {len(files)} Python files, {sum(f.stat().st_size for f in out.rglob('*') if f.is_file()) / 1e6:.1f} MB")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pyodide", type=Path, required=True, help="the pyodide npm package directory")
    ap.add_argument("--out", type=Path, default=ROOT / "_site")
    args = ap.parse_args()
    build(args.pyodide, args.out)


if __name__ == "__main__":
    main()
