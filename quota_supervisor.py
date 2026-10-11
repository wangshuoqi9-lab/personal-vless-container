"""Fail-closed Xray supervisor backed by durable, prepaid byte grants.

Only the central ledger starts a new allowance. Container restarts receive a new
instance identity and cannot recover an old grant. The entire container NIC TX
counter is charged, including proxy framing and control-plane traffic.
"""
import multiprocessing
import tempfile
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


API_DEADLINE_SECONDS = 6.0
API_RESULT_LIMIT = 16 * 1024


def _quota_call_worker(output, function, args):
    # The production parent has no worker threads. Fork keeps the credential
    # out of argv/env/files; only a bounded response uses an anonymous file.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    try:
        status, body = function(*args)
        if type(status) is not int or not 100 <= status <= 599 or not isinstance(body, dict):
            raise ValueError()
        result = {"status": status, "body": body}
    except (OSError, TimeoutError, urllib.error.URLError):
        result = {"failure": "quota_request_io"}
    except Exception:
        result = {"failure": "quota_request_invalid_reply"}
    raw = json.dumps(result, separators=(",", ":")).encode("utf8")
    if len(raw) > API_RESULT_LIMIT:
        raw = b'{"failure":"quota_request_invalid_reply"}'
    output.write(raw)
    output.flush()


class QuotaProcessFuture:
    def __init__(self, function, args, timeout=API_DEADLINE_SECONDS):
        self.deadline = time.monotonic() + timeout
        self.wall_deadline = time.time() + timeout
        self.finished = False
        self.value = None
        self.failure = None
        self.output = tempfile.TemporaryFile(mode="w+b")
        self.process = multiprocessing.get_context("fork").Process(
            target=_quota_call_worker, args=(self.output, function, args), daemon=True)
        try:
            self.process.start()
        except BaseException:
            self.output.close()
            self.process.close()
            raise

    def _reap(self, kill=False):
        if kill and self.process.is_alive():
            self.process.kill()
        self.process.join(timeout=0.25)
        if self.process.is_alive():
            # Never create a replacement while a prior request still exists.
            raise QuotaError("quota_worker_cleanup_failed")
        self.process.close()

    def done(self):
        if self.finished:
            return True
        if time.monotonic() >= self.deadline or time.time() >= self.wall_deadline:
            self._reap(kill=True)
            self.failure = "quota_request_deadline"
        elif self.process.is_alive():
            return False
        else:
            exitcode = self.process.exitcode
            self._reap()
            self.output.seek(0)
            raw = self.output.read(API_RESULT_LIMIT + 1)
            try:
                value = json.loads(raw)
                if exitcode != 0 or len(raw) > API_RESULT_LIMIT or not isinstance(value, dict):
                    raise ValueError()
                if "failure" in value:
                    if value["failure"] not in {"quota_request_io", "quota_request_invalid_reply"}:
                        raise ValueError()
                    self.failure = value["failure"]
                elif type(value.get("status")) is int and 100 <= value["status"] <= 599 and isinstance(value.get("body"), dict):
                    self.value = (value["status"], value["body"])
                else:
                    raise ValueError()
            except (ValueError, UnicodeError):
                self.failure = "quota_request_worker_failed"
        self.output.close()
        self.finished = True
        return True

    def result(self):
        if not self.done():
            raise QuotaError("quota_request_not_ready")
        if self.failure:
            raise QuotaError(self.failure)
        return self.value

    def cancel(self):
        if not self.finished:
            try:
                self._reap(kill=True)
            finally:
                self.output.close()
            self.failure = "quota_request_cancelled"
            self.finished = True


class QuotaProcessPool:
    def __init__(self):
        self.pending = None

    def submit(self, function, *args):
        if self.pending is not None and not self.pending.done():
            raise QuotaError("quota_request_already_pending")
        self.pending = QuotaProcessFuture(function, args)
        return self.pending

    def shutdown(self, **_kwargs):
        if self.pending is not None:
            self.pending.cancel()


class Meter:
    def __init__(self, path=Path("/proc/self/net/dev")):
        # proc/self observes this process's network namespace, even when a
        # platform masks or exposes a different view through /sys/class/net.
        # Select this source once; never silently switch or reset its baseline.
        self.path = Path(path)
        self.baseline = self.read()
        self.last = self.baseline.copy()
        print("category=quota_meter_ready source=proc_self_net_dev "
              f"interfaceCount={len(self.baseline)}", flush=True)

    def read(self):
        with self.path.open("rb") as file:
            raw = file.read(65537)
        if len(raw) > 65536:
            raise QuotaError("invalid_network_meter")
        try:
            lines = raw.decode("ascii").splitlines()
        except UnicodeError:
            raise QuotaError("invalid_network_meter") from None
        expected = (
            ["Inter-", "Receive", "Transmit"],
            ["face", "bytes packets errs drop fifo frame compressed multicast",
             "bytes packets errs drop fifo colls carrier compressed"],
        )
        if len(lines) < 2 or any(
                [" ".join(part.split()) for part in lines[index].split("|")] != header
                for index, header in enumerate(expected)):
            raise QuotaError("invalid_network_meter")
        counters, seen = {}, set()
        for line in lines[2:]:
            name, separator, values = line.partition(":")
            name, columns = name.strip(), values.split()
            if (not separator or not re.fullmatch(r"[^\s:]{1,15}", name)
                    or name in seen or len(columns) != 16
                    or any(not re.fullmatch(r"[0-9]{1,20}", item) for item in columns)):
                raise QuotaError("invalid_network_meter")
            numbers = [int(item) for item in columns]
            if any(item > (1 << 64) - 1 for item in numbers):
                raise QuotaError("invalid_network_meter")
            seen.add(name)
            if name != "lo":
                # All non-loopback interfaces are charged. Layered virtual
                # interfaces may overcount; selecting just one could undercount.
                counters[name] = numbers[8]
        if not counters:
            raise QuotaError("ambiguous_network_meter")
        return counters

    def _sample(self):
        counters = self.read()
        if counters.keys() != self.baseline.keys():
            raise QuotaError("network_meter_interfaces_changed")
        # Check each interface: growth elsewhere must not mask one counter reset.
        if any(counters[name] < self.last[name] for name in counters):
            raise QuotaError("network_counter_regressed")
        self.last = counters.copy()
        return counters

    def used(self):
        counters = self._sample()
        return sum(value - self.baseline[name] for name, value in counters.items())

    def verify_activity(self):
        # A completed HTTPS lease has transmitted bytes. Never open a proxy on
        # an unproven/stub counter that stayed at zero throughout that request.
        used = self.used()
        if used <= 0:
            raise QuotaError("network_meter_inactive")
        return used

    def new_period(self):
        # The caller has already stopped the core and accepted a server period
        # transition. Still reject interface replacement or counter regression.
        self.baseline = self._sample().copy()


class Grant:
    def __init__(self):
        self.instance = str(uuid.uuid4())
        self.period = None
        self.sequence = 0
        self.total = 0
        self.deadline = 0.0
        self.expires_at = 0.0
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
        self.expires_at = expires
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
        self.expires_at = expires

    def admissible(self, used, monotonic=None):
        moment = time.monotonic() if monotonic is None else monotonic
        return (not self.exhausted and self.period is not None and used + RESERVE < self.total
                and moment < self.deadline and time.time() < min(self.expires_at, self.reset_at))


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
    next_request_wall = next_heartbeat_wall = 0.0
    blocked_reported = False
    pool = QuotaProcessPool()
    next_watchdog = next_watchdog_wall = 0.0
    last_watchdog_mono, last_watchdog_wall = time.monotonic(), time.time()
    core_env = {k: v for k, v in env.items() if k not in ("VLESS_UUID", "NODE_QUOTA_TOKEN")}
    try:
        while not STOP.wait(0.1):
            used, moment = meter.used(), time.monotonic()
            wall_now = time.time()
            if moment >= next_watchdog or wall_now >= next_watchdog_wall:
                remaining = max(0, int(min(grant.deadline - moment,
                    grant.expires_at - time.time(), grant.reset_at - time.time())))
                print("category=quota_watchdog "
                      f"core={'open' if child is not None else 'closed'} "
                      f"api={'pending' if pending is not None else 'idle'} "
                      f"remainingSeconds={remaining} "
                      f"wallElapsed={int(wall_now - last_watchdog_wall)} "
                      f"monoElapsed={int(moment - last_watchdog_mono)}", flush=True)
                last_watchdog_wall, last_watchdog_mono = wall_now, moment
                next_watchdog, next_watchdog_wall = moment + 60, wall_now + 60
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
                    if status != 200:
                        print(f"category=quota_api_response action={action} status={status}", flush=True)
                    if status == 409 and result.get("error") == "new_instance_required" and grant.period is None:
                        # The first lease may have committed just before a
                        # month boundary while its response was lost. It is
                        # safe to abandon that identity: no core was opened.
                        assert child is None
                        grant = Grant()
                        meter.new_period()
                    elif status == 409 and result.get("error") == "instance_superseded":
                        grant.exhausted = True
                    elif status == 409 and re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", str(result.get("currentPeriod", ""))):
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
                    retry_delay = 30 if grant.exhausted else 0
                    next_request, next_request_wall = moment + retry_delay, wall_now + retry_delay
                except (OSError, ValueError, QuotaError, TimeoutError, urllib.error.URLError) as error:
                    allowed = {"quota_request_deadline", "quota_request_io",
                               "quota_request_invalid_reply", "quota_request_worker_failed"}
                    category = str(error) if isinstance(error, QuotaError) and str(error) in allowed else "quota_request_failed"
                    print(f"category={category} action={action}", flush=True)
                    # Retry exactly the same sequence after an uncertain result.
                    # Metering and the existing deadline continue meanwhile.
                    next_request, next_request_wall = moment + 10, wall_now + 10
                pending = pending_action = None

            used = meter.used()
            if child is None and grant.admissible(used):
                used = meter.verify_activity()
                if not grant.admissible(used):
                    continue
                child = subprocess.Popen([xray, "run", "-c", str(config)],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, env=core_env)
                next_heartbeat = next_heartbeat_wall = 0
                blocked_reported = False
                print("category=quota_proxy_open", flush=True)

            urgent_close = grant.period is not None and child is None and not blocked_reported
            if pending is None and (moment >= next_request or wall_now >= next_request_wall or urgent_close):
                if grant.period and (moment >= next_heartbeat or wall_now >= next_heartbeat_wall
                                     or (child is None and not blocked_reported)):
                    pending_action = "status"
                    pending = pool.submit(api.call, "status", {"instance": grant.instance,
                        "period": grant.period, "usedBytes": str(used), "blocked": child is None})
                    next_heartbeat, next_heartbeat_wall = moment + 30, wall_now + 30
                    blocked_reported = child is None
                elif (not grant.exhausted and (grant.period is None or grant.total - used <= RENEW_AT)):
                    pending_action = "lease"
                    pending = pool.submit(api.call, "lease", grant.request())
                elif grant.exhausted and (moment >= next_heartbeat or wall_now >= next_heartbeat_wall):
                    # Lease-less startup after exhaustion still rechecks with
                    # the central ledger; no local allowance can be invented.
                    pending_action = "lease"
                    pending = pool.submit(api.call, "lease", grant.request())
                    next_heartbeat, next_heartbeat_wall = moment + 30, wall_now + 30
        return 0
    finally:
        try:
            stop_core(child)
        finally:
            pool.shutdown(wait=True, cancel_futures=True)


def install_signals():
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: STOP.set())
