"""Did the PSP possibly get our request?

Tested with Toxiproxy injecting real TCP faults: after a reset or a slow reply the PSP had
received the request. Only a failure before the connection opened proves it didn't, because
request bytes are sent only after the connection opens.
"""

from __future__ import annotations

import requests
from urllib3.exceptions import NewConnectionError


def classify(exc: Exception) -> str:
    """'not_delivered' only when the connection never opened; 'unknown' for everything else."""
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return "not_delivered"
    if isinstance(exc, requests.exceptions.ConnectionError):
        reason = getattr(exc.args[0], "reason", None) if exc.args else None
        if isinstance(reason, NewConnectionError):  # e.g. connection refused
            return "not_delivered"
    return "unknown"  # read timeout, reset mid-request, broken reply
