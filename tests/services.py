"""Start real PostgreSQL, Redis and Toxiproxy servers for the tests, or use ones you point at.

    SETTLE_TEST_DATABASE_URL   use this PostgreSQL instead of starting one
    SETTLE_TEST_REDIS_URL      use this Redis instead of starting one
    SETTLE_TEST_TOXIPROXY      use this Toxiproxy API (e.g. http://localhost:8474)

Otherwise the binaries are found on PATH (and PostgreSQL under /usr/lib/postgresql) and started
on free ports in a temporary directory. Tests that need a server it cannot get are skipped.
"""

from __future__ import annotations

import glob
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg
import requests

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait_port(port: int, timeout: float = 15.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"nothing listening on {port}")


def _pg_bin(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    hits = sorted(glob.glob(f"/usr/lib/postgresql/*/bin/{name}"))
    return hits[-1] if hits else None


class Postgres:
    def __init__(self) -> None:
        self.proc: subprocess.Popen[bytes] | None = None
        self.dir: str | None = None
        env_url = os.environ.get("SETTLE_TEST_DATABASE_URL")
        if env_url:
            self.dsn = env_url
            return
        initdb, postgres = _pg_bin("initdb"), _pg_bin("postgres")
        if not initdb or not postgres:
            raise RuntimeError("no PostgreSQL binaries")
        self.dir = tempfile.mkdtemp(prefix="settle-pg-")
        prefix: list[str] = []
        if os.geteuid() == 0:  # PostgreSQL refuses to run as root
            shutil.chown(self.dir, "postgres", "postgres")
            prefix = ["runuser", "-u", "postgres", "--"]
        data = os.path.join(self.dir, "data")
        subprocess.run(prefix + [initdb, "-D", data, "-U", "postgres", "--auth=trust", "-E", "UTF8"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.port = free_port()
        self.proc = subprocess.Popen(
            prefix + [postgres, "-D", data, "-p", str(self.port), "-k", self.dir, "-c", "listen_addresses=127.0.0.1",
                      "-c", "fsync=off", "-c", "max_connections=300", "-c", "tcp_keepalives_idle=5",
                      "-c", "tcp_keepalives_interval=2", "-c", "tcp_keepalives_count=3"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        wait_port(self.port)
        admin = f"postgresql://postgres@127.0.0.1:{self.port}/postgres"
        for _ in range(50):
            try:
                with psycopg.connect(admin, autocommit=True) as c:
                    c.execute("CREATE DATABASE settle")
                break
            except psycopg.OperationalError:
                time.sleep(0.1)
        self.dsn = f"postgresql://postgres@127.0.0.1:{self.port}/settle"

    def stop(self) -> None:
        if self.proc:
            self.proc.terminate()
            self.proc.wait(10)
        if self.dir:
            shutil.rmtree(self.dir, ignore_errors=True)


class Redis:
    def __init__(self) -> None:
        self.proc: subprocess.Popen[bytes] | None = None
        env_url = os.environ.get("SETTLE_TEST_REDIS_URL")
        if env_url:
            self.url = env_url
            return
        binary = shutil.which("redis-server")
        if not binary:
            raise RuntimeError("no redis-server")
        self.port = free_port()
        self.binary = binary
        self.start()
        self.url = f"redis://127.0.0.1:{self.port}/0"

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [self.binary, "--port", str(self.port), "--save", "", "--appendonly", "no", "--bind", "127.0.0.1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        wait_port(self.port)

    def stop(self) -> None:
        if self.proc:
            self.proc.terminate()
            self.proc.wait(10)
            self.proc = None


def toxiproxy_binary() -> str | None:
    return shutil.which("toxiproxy-server") or next(
        (p for p in [str(ROOT / "bin" / "toxiproxy-server")] if os.path.exists(p)),
        None,
    )


class Toxiproxy:
    def __init__(self) -> None:
        self.proc: subprocess.Popen[bytes] | None = None
        env_url = os.environ.get("SETTLE_TEST_TOXIPROXY")
        if env_url:
            self.api = env_url.rstrip("/")
            return
        binary = toxiproxy_binary()
        if not binary:
            raise RuntimeError("no toxiproxy-server")
        port = free_port()
        self.proc = subprocess.Popen([binary, "-host", "127.0.0.1", "-port", str(port)],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_port(port)
        self.api = f"http://127.0.0.1:{port}"

    def http(self) -> requests.Session:
        s = requests.Session()
        s.trust_env = False
        return s

    def proxy(self, name: str, upstream: str) -> int:
        """Create (or recreate) a proxy and return the port it listens on."""
        s = self.http()
        s.delete(f"{self.api}/proxies/{name}")
        listen = free_port()
        r = s.post(f"{self.api}/proxies", json={"name": name, "listen": f"127.0.0.1:{listen}", "upstream": upstream})
        r.raise_for_status()
        return listen

    def toxic(self, proxy: str, kind: str, stream: str = "downstream", **attributes: object) -> None:
        r = self.http().post(f"{self.api}/proxies/{proxy}/toxics",
                             json={"type": kind, "stream": stream, "attributes": attributes})
        r.raise_for_status()

    def enable(self, proxy: str, enabled: bool) -> None:
        self.http().post(f"{self.api}/proxies/{proxy}", json={"enabled": enabled}).raise_for_status()

    def reset(self) -> None:
        self.http().post(f"{self.api}/reset").raise_for_status()

    def stop(self) -> None:
        if self.proc:
            self.proc.terminate()
            self.proc.wait(10)
