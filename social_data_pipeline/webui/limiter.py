"""Failed logins per client address: at most five in five minutes from one address, an IPv4 address taken as it is and an IPv6 address by its /64 network, in a table of at most 4096 addresses (the one changed longest ago leaves first). Best effort: the address can be the one Docker's proxy dials from, and one IPv6 /48 holds 65,536 /64 keys; the broker's two login budgets are the real bound. Every login with both signs of the machine itself (`app.arrived_locally`) is counted under one key, `LOCAL_KEY`; any other connection is counted under its own address.

A login attempt takes a place in its address's list before the broker is asked (`begin`), so that attempts sent at the same time cannot together pass the limit; `end` gives the place back unless the attempt failed. A refused attempt asks the broker nothing. A success clears nothing: neither its own address's failures nor another's. The clock is a parameter (seconds, never going back). Standard library only.
"""

import ipaddress
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass

FAILURES_MAX = 5
WINDOW_SECONDS = 5 * 60
ADDRESSES_MAX = 4096
# The key of a client address that is not an IP address (none, in a real deployment).
UNKNOWN = "unknown"
# The one key of every login with both signs of the machine itself (`app.arrived_locally`); any other connection is counted under its own address.
LOCAL_KEY = "local port"


def address_key(address):
    """The key a client address is counted under: `LOCAL_KEY` as it is, an IPv4 address (an IPv4 address in IPv6 form included) as it is, an IPv6 address by its /64 network, anything else as one shared key."""
    if address == LOCAL_KEY:
        return LOCAL_KEY
    try:
        parsed = ipaddress.ip_address(address)
    except (ValueError, TypeError):
        return UNKNOWN
    if parsed.version == 6:
        if parsed.ipv4_mapped is not None:
            return str(parsed.ipv4_mapped)
        return str(ipaddress.IPv6Network((int(parsed) >> 64 << 64, 64)))
    return str(parsed)


@dataclass
class Attempt:
    """A place taken by `begin`: the key and the time it holds."""

    key: str
    at: float


class LoginLimiter:
    def __init__(self, clock, *, failures_max=FAILURES_MAX, window_seconds=WINDOW_SECONDS, addresses_max=ADDRESSES_MAX):
        self._clock = clock
        self._failures_max = failures_max
        self._window = window_seconds
        self._addresses_max = addresses_max
        self._lock = threading.Lock()
        self._table = OrderedDict()

    def _recent(self, key, now):
        times = self._table.get(key)
        if times is None:
            return None
        while times and now - times[0] >= self._window:
            times.popleft()
        if not times:
            del self._table[key]
            return None
        return times

    def begin(self, address):
        """Take a place for a login from `address`: an `Attempt`, or None when the address has had its five failures (counting the attempts under way) in the last five minutes."""
        key = address_key(address)
        with self._lock:
            now = self._clock()
            times = self._recent(key, now)
            if times is not None and len(times) >= self._failures_max:
                return None
            if times is None:
                while len(self._table) >= self._addresses_max:
                    self._table.popitem(last=False)
                times = self._table[key] = deque()
            times.append(now)
            self._table.move_to_end(key)
            return Attempt(key, now)

    def end(self, attempt, failed):
        """End an attempt: a failure keeps its place for five minutes; anything else gives it back."""
        if failed:
            return
        with self._lock:
            times = self._table.get(attempt.key)
            if times is None:
                return
            try:
                times.remove(attempt.at)
            except ValueError:
                return
            if not times:
                del self._table[attempt.key]

    def addresses(self):
        """How many addresses the table holds."""
        with self._lock:
            return len(self._table)
