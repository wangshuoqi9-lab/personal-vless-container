"""Render one private VLESS/WS inbound, then replace PID 1 with Xray.

TLS terminates at the platform's HTTPS ingress. There is no public subscription
endpoint, command execution endpoint, background agent, or runtime downloader.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid

XRAY = "/usr/local/bin/xray"


def build_config(env):
    try:
        identity = uuid.UUID(env.get("VLESS_UUID", ""))
    except (ValueError, AttributeError):
        raise ValueError("VLESS_UUID must be a newly generated UUID v4") from None
    if identity.version != 4:
        raise ValueError("VLESS_UUID must be a newly generated UUID v4")
    raw_port = env.get("PORT", "8080")
    if not re.fullmatch(r"[0-9]{1,5}", raw_port):
        raise ValueError("PORT must be an integer from 1024 to 65535")
    port = int(raw_port)
    if not 1024 <= port <= 65535:
        raise ValueError("PORT must be an integer from 1024 to 65535")
    path = env.get("WS_PATH", "/video-ws")
    if not re.fullmatch(r"/[A-Za-z0-9/_-]{1,127}", path):
        raise ValueError("WS_PATH must be a plain slash-prefixed path of 2 to 128 characters")
    return {
        "log": {"access": "none", "error": "none", "loglevel": "none", "dnsLog": False},
        "inbounds": [{
            "tag": "personal-video-ws", "listen": "0.0.0.0", "port": port,
            "protocol": "vless",
            "settings": {"clients": [{"id": str(identity)}], "decryption": "none"},
            "streamSettings": {"network": "ws", "security": "none", "wsSettings": {"path": path}},
        }],
        "outbounds": [{"tag": "direct", "protocol": "freedom"}, {"tag": "blocked", "protocol": "blackhole"}],
        "routing": {
            "domainStrategy": "IPOnDemand",
            "rules": [{"type": "field", "ip": [
                "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
                "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/4",
                "::/128", "::1/128", "fc00::/7", "fe80::/10", "ff00::/8",
            ], "outboundTag": "blocked"}],
        },
    }


def main():
    os.umask(0o077)
    try:
        config = build_config(os.environ)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 78
    directory = Path(tempfile.mkdtemp(prefix="xray-private-"))
    path = directory / "config.json"
    with path.open("x", encoding="utf-8") as file:
        json.dump(config, file, separators=(",", ":"))
    result = subprocess.run([XRAY, "run", "-test", "-c", str(path)],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode:
        path.unlink(missing_ok=True)
        directory.rmdir()
        print("Xray configuration validation failed", file=sys.stderr)
        return 78
    child_env = dict(os.environ)
    child_env.pop("VLESS_UUID", None)
    print("Starting configured personal VLESS WebSocket service", flush=True)
    os.execve(XRAY, [XRAY, "run", "-c", str(path)], child_env)


if __name__ == "__main__":
    sys.exit(main())
