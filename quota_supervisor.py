"""Fail-closed Xray supervisor backed by durable, prepaid byte grants.

Only the central ledger starts a new allowance. Container restarts receive a new
instance identity and cannot recover an old grant. The entire container NIC TX
counter is charged, including proxy framing and control-plane traffic.
"""
import concurrent.futures
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


MIB = 1024 * 1024
RESERVE = 4 * MIB
RENEW_AT = 24 * MIB
MAX_BYTES = 1_900_000_000_000
STOP = threading.Event()


class QuotaError(Exception):
    pass


def byte_count(value):
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,13}", value):
        raise QuotaError("invalid_byte_count")
    result = int(value)
    if result > MAX_BYTES:
        raise QuotaError("invalid_byte_count")
    return result


def utc_seconds(value):
    if not isinstance(value, str):
        raise QuotaError("invalid_deadline")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.utcoffset() is None:
            raise ValueError()
        return stamp.timestamp()
    except ValueError:
        raise QuotaError("invalid_deadline") from None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise QuotaError("unexpected_redirect")


class QuotaAPI:
    def __init__(self, env):
        self.url = env.get("NODE_QUOTA_URL", "").rstrip("/")
        self.node = env.get("NODE_QUOTA_ID", "")
        self.token = env.get("NODE_QUOTA_TOKEN", "")
        url = urllib.parse.urlsplit(self.url)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.query or url.fragment or not re.fullmatch(r"[a-z0-9-]{1,32}", self.node)
                or not 32 <= len(self.token) <= 256 or re.search(r"[\s\x00-\x1f]", self.token)):
            raise QuotaError("quota_configuration_missing_or_invalid")
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def call(self, action, body):
        request = urllib.request.Request(self.url + "/" + action + "/" + self.node,
            data=json.dumps(body, separators=(",", ":")).encode(),
            headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"},
            method="POST")
        try:
            with self.opener.open(request, timeout=4) as response:
                status, raw = response.status, response.read(8193)
        except urllib.error.HTTPError as response:
            status, raw = response.code, response.read(8193)
        if len(raw) > 8192:
            raise QuotaError("quota_response_too_large")
        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError()
        except (ValueError, UnicodeError):
            raise QuotaError("invalid_quota_response") from None
        return status, body


class Meter:
    def __init__(self, root=Path("/sys/class/net")):
        interfaces = sorted(p for p in root.iterdir() if p.name != "lo")
        # The pod must have an unambiguous interface; never fall back to a
        # process counter or silently undercount a second interface.
        if len(interfaces) != 1:
            raise QuotaError("ambiguous_network_meter")
        self.path = interfaces[0] / "statistics/tx_bytes"
        self.baseline = self.last = self.read()

    def read(self):
        raw = self.path.read_text().strip()
        if not re.fullmatch(r"[0-9]{1,22}", raw):
            raise QuotaError("invalid_network_meter")
        return int(raw)

    def used(self):
        counter = self.read()
        if counter < self.last:
            raise QuotaError("network_counter_regressed")
        self.last = counter
        return counter - self.baseline

    def new_period(self):
        self.baseline = self.last = self.read()


class Grant:
    def __init__(self):
        self.instance = str(uuid.uuid4())
        self.period = None
        self.sequence = 0
        self.total = 0
        self.deadline = 0.0
        self.reset_at = 0.0
        self.exhausted = False

    def request(self):
        return {"instance": self.instance, "sequence": self.sequence + 1,
                **({"period": self.period} if self.period else {})}

    def accept(self, value, now=None, monotonic=None):
        now = time.time() if now is None else now
        monotonic = time.monotonic() if monotonic is None else monotonic
        period = value.get("period")
        sequence = value.get("sequence")
        if not isinstance(period, str) or not re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", period):
            raise QuotaError("invalid_grant_period")
        if self.period and period != self.period:
            raise QuotaError("grant_period_changed")
        if type(sequence) is not int or sequence != self.sequence + 1:
            raise QuotaError("invalid_grant_sequence")
        amount = byte_count(value.get("grantBytes"))
        total = byte_count(value.get("totalGrantedBytes"))
        if amount > 64 * MIB or total != self.total + amount:
            raise QuotaError("invalid_grant_amount")
        reset_at, expires = utc_seconds(value.get("resetAt")), utc_seconds(value.get("expiresAt"))
        if expires > reset_at or not 0 < expires - now <= 660:
            raise QuotaError("invalid_grant_expiry")
        self.period, self.sequence, self.total = period, sequence, total
        self.reset_at, self.deadline = reset_at, monotonic + expires - now
        self.exhausted = amount == 0

    def heartbeat(self, value, now=None, monotonic=None):
        now = time.time() if now is None else now
        monotonic = time.monotonic() if monotonic is None else monotonic
        if value.get("quotaBlocked") is True:
            self.exhausted = True
            return
        expires = utc_seconds(value.get("expiresAt"))
        if expires > self.reset_at or not 0 < expires - now <= 660:
            raise QuotaError("invalid_heartbeat_expiry")
        self.deadline = monotonic + expires - now

    def admissible(self, used, monotonic=None):
        moment = time.monotonic() if monotonic is None else monotonic
        return (not self.exhausted and self.period is not None and used + RESERVE < self.total
                and moment < self.deadline and time.time() < self.reset_at)


def stop_core(child):
    if child is not None and child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=2)


def supervise(xray, config, env):
    """Return only on shutdown or unsafe metering/configuration; no raw logs."""
    api, meter, grant = QuotaAPI(env), Meter(), Grant()
    child = None
    pending = None
    pending_action = None
    next_request = next_heartbeat = 0.0
    blocked_reported = False
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    core_env = {k: v for k, v in env.items() if k not in ("VLESS_UUID", "NODE_QUOTA_TOKEN")}
    try:
        while not STOP.wait(0.1):
            used, moment = meter.used(), time.monotonic()
            if not grant.admissible(used, moment):
                if child is not None:
                    stop_core(child)
                    child = None
                    blocked_reported = False
                    print("category=quota_proxy_closed", flush=True)
            if child is not None and child.poll() is not None:
                raise QuotaError("proxy_exited")

            if pending is not None and pending.done():
                action = pending_action
                try:
                    status, result = pending.result()
                    if status == 409 and re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", str(result.get("currentPeriod", ""))):
                        # Changing period is only accepted after the old process
                        # is gone. The server, not a local month check, grants it.
                        if grant.period and result["currentPeriod"] != grant.period:
                            stop_core(child)
                            child = None
                            grant = Grant()
                            meter.new_period()
                            used = meter.used()
                            blocked_reported = False
                    elif status == 200:
                        if action == "lease":
                            grant.accept(result)
                        else:
                            grant.heartbeat(result)
                    elif status in (401, 403):
                        grant.exhausted = True
                    else:
                        raise QuotaError("quota_service_unavailable")
                    next_request = moment + (30 if grant.exhausted else 0)
                except (OSError, ValueError, QuotaError, TimeoutError, urllib.error.URLError):
                    # Retry exactly the same sequence after an uncertain result.
                    # Metering and the existing deadline continue meanwhile.
                    next_request = moment + 10
                pending = pending_action = None

            used = meter.used()
            if child is None and grant.admissible(used):
                child = subprocess.Popen([xray, "run", "-c", str(config)],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, env=core_env)
                next_heartbeat = 0
                blocked_reported = False
                print("category=quota_proxy_open", flush=True)

            urgent_close = grant.period is not None and child is None and not blocked_reported
            if pending is None and (moment >= next_request or urgent_close):
                if grant.period and (moment >= next_heartbeat or (child is None and not blocked_reported)):
                    pending_action = "status"
                    pending = pool.submit(api.call, "status", {"instance": grant.instance,
                        "period": grant.period, "usedBytes": str(used), "blocked": child is None})
                    next_heartbeat = moment + 30
                    blocked_reported = child is None
                elif (not grant.exhausted and (grant.period is None or grant.total - used <= RENEW_AT)):
                    pending_action = "lease"
                    pending = pool.submit(api.call, "lease", grant.request())
                elif grant.exhausted and moment >= next_heartbeat:
                    # Lease-less startup after exhaustion still rechecks with
                    # the central ledger; no local allowance can be invented.
                    pending_action = "lease"
                    pending = pool.submit(api.call, "lease", grant.request())
                    next_heartbeat = moment + 30
        return 0
    finally:
        stop_core(child)
        pool.shutdown(wait=True, cancel_futures=True)


def install_signals():
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: STOP.set())
