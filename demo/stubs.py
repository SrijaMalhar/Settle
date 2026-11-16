"""Stand-ins for the network libraries in the browser build.

The live world runs the engine single-threaded with scheduled events instead of sockets, so
nothing here is ever called. The modules only have to import: settle.adapters.notifier posts
merchant webhooks with requests, and settle.mockpsp serves its HTTP API with Flask.
"""

from __future__ import annotations

import sys
import types
from typing import Any


def _never(*_: Any, **__: Any) -> Any:
    raise RuntimeError("no network in the browser build")


class _Stub:
    def __init__(self, *_: Any, **__: Any) -> None:
        _never()


def install() -> None:
    requests = types.ModuleType("requests")
    adapters = types.ModuleType("requests.adapters")
    adapters.HTTPAdapter = _Stub  # type: ignore[attr-defined]
    requests.adapters = adapters  # type: ignore[attr-defined]
    requests.Session = _Stub  # type: ignore[attr-defined]
    requests.post = requests.get = _never  # type: ignore[attr-defined]
    requests.RequestException = OSError  # type: ignore[attr-defined]
    flask = types.ModuleType("flask")
    flask.Flask = _Stub  # type: ignore[attr-defined]
    flask.jsonify = _never  # type: ignore[attr-defined]
    flask.request = None  # type: ignore[attr-defined]
    typing_ = types.ModuleType("flask.typing")
    typing_.ResponseReturnValue = object  # type: ignore[attr-defined]
    for name, mod in (("requests", requests), ("requests.adapters", adapters), ("flask", flask),
                      ("flask.typing", typing_)):
        sys.modules.setdefault(name, mod)
