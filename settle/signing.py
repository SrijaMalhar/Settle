"""PSP callbacks are signed with HMAC-SHA256 over the raw body, the way real PSP webhooks are."""

from __future__ import annotations

import hashlib
import hmac

HEADER = "X-Settle-Signature"


def sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(secret: str, body: bytes, signature: str | None) -> bool:
    return signature is not None and hmac.compare_digest(sign(secret, body), signature)
