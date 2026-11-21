"""Run: python -m app.internal_read_v1 — see INTERNAL_READ_HOST / INTERNAL_READ_PORT / PORT."""

from __future__ import annotations

import os

import uvicorn

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _listen_host() -> str:
    # Local default: loopback only. Railway/production: set INTERNAL_READ_HOST=0.0.0.0
    return os.environ.get("INTERNAL_READ_HOST", "127.0.0.1")


def _listen_port() -> int:
    # Railway injects PORT; INTERNAL_READ_PORT overrides when set explicitly.
    raw = os.environ.get("INTERNAL_READ_PORT") or os.environ.get("PORT") or "8090"
    return int(raw)


def _dual_stack_socket(port: int):
    """One IPv6 socket that also accepts IPv4. asyncio sets IPV6_V6ONLY on '::' listeners,
    which serves Railway's IPv6 private network but drops its IPv4 public edge."""
    import socket

    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
    sock.bind(("::", port))
    sock.listen(2048)
    sock.set_inheritable(True)
    return sock


if __name__ == "__main__":
    host, port = _listen_host(), _listen_port()
    if host == "::":
        config = uvicorn.Config("app.internal_read_v1.app:app", host=host, port=port, factory=False)
        uvicorn.Server(config).run(sockets=[_dual_stack_socket(port)])
    else:
        uvicorn.run(
            "app.internal_read_v1.app:app",
            host=host,
            port=port,
            factory=False,
        )
