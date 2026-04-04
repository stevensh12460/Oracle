"""
zmq_subscriber.py - ZMQ SUB subscriber for ORACLE.

Connects to Nikita's ZMQ PUB on tcp://192.168.158.237:5556 and caches the
latest state per topic.  Subscriber runs in a daemon thread — no-op if pyzmq
is not installed.

Topics consumed: SIGNAL, ENRICHMENT, HEALTH
"""

import json
import logging
import threading
import time

try:
    import zmq
    _ZMQ_AVAILABLE = True
except ImportError:
    _ZMQ_AVAILABLE = False

logger = logging.getLogger("zmq_subscriber")

NIKITA_ZMQ_ADDR = "tcp://192.168.158.237:5556"
DEFAULT_TOPICS = ["SIGNAL", "ENRICHMENT", "HEALTH"]


class NikitaSubscriber:
    """Thin ZMQ SUB client that caches the latest message per topic/key."""

    def __init__(self, address: str = NIKITA_ZMQ_ADDR, topics: list[str] | None = None):
        self._address = address
        self._topics = topics if topics is not None else DEFAULT_TOPICS
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._cache: dict[str, dict] = {
            "signals": {},       # signal_type -> latest payload
            "enrichment": {},    # mode -> latest payload
            "health": {},        # ts -> latest payload
            "prices": {},        # symbol -> latest price (float)
        }

        if not _ZMQ_AVAILABLE:
            logger.warning(
                "[ZMQ SUB] pyzmq not installed — subscriber is a no-op. "
                "Install with: pip install pyzmq"
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if not _ZMQ_AVAILABLE:
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="zmq-sub")
        self._thread.start()
        logger.info("[ZMQ SUB] Connecting to %s, topics=%s", self._address, self._topics)

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None

    # ------------------------------------------------------------------
    # Public getters (thread-safe)
    # ------------------------------------------------------------------

    def get_latest_signal(self, signal_type: str) -> dict | None:
        """Return the most recent SIGNAL payload for the given type (e.g. 'BUY')."""
        with self._lock:
            return dict(self._cache["signals"].get(signal_type, {})) or None

    def get_latest_enrichment(self, mode: str) -> dict | None:
        """Return the most recent ENRICHMENT payload for the given mode."""
        with self._lock:
            return dict(self._cache["enrichment"].get(mode, {})) or None

    def get_health(self) -> dict:
        """Return the most recent HEALTH payload."""
        with self._lock:
            return dict(self._cache["health"])

    def get_price(self, symbol: str) -> float | None:
        """Return the latest price for a symbol, or None if not yet received."""
        with self._lock:
            return self._cache["prices"].get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        """Return a snapshot of all cached prices."""
        with self._lock:
            return dict(self._cache["prices"])

    # ------------------------------------------------------------------
    # Internal: background receive loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVTIMEO, 1000)  # 1-second recv timeout so we can check _running
        sock.setsockopt(zmq.LINGER, 0)

        for topic in self._topics:
            sock.setsockopt_string(zmq.SUBSCRIBE, topic)

        try:
            sock.connect(self._address)
        except Exception:
            logger.exception("[ZMQ SUB] Failed to connect to %s", self._address)
            self._running = False
            return

        while self._running:
            try:
                raw = sock.recv()
                self._handle(raw)
            except zmq.Again:
                # Timeout — loop and check _running
                continue
            except Exception:
                logger.exception("[ZMQ SUB] Receive error")

        sock.close()
        ctx.term()
        logger.info("[ZMQ SUB] Shut down cleanly")

    def _handle(self, raw: bytes) -> None:
        try:
            space = raw.index(b" ")
            topic = raw[:space].decode("utf-8")
            payload = json.loads(raw[space + 1:])
        except Exception:
            return

        with self._lock:
            if topic == "PRICE":
                sym = payload.get("symbol")
                price = payload.get("price")
                if sym and price is not None:
                    self._cache["prices"][sym] = price
            elif topic == "RESYNC":
                prices = payload.get("prices", {})
                self._cache["prices"].update(prices)
            elif topic == "SIGNAL":
                sig_type = payload.get("type", "UNKNOWN")
                self._cache["signals"][sig_type] = payload
            elif topic == "ENRICHMENT":
                mode = payload.get("mode", "UNKNOWN")
                self._cache["enrichment"][mode] = payload
            elif topic == "HEALTH":
                self._cache["health"] = payload


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_instance: NikitaSubscriber | None = None
_instance_lock = threading.Lock()


def get_subscriber(address: str = NIKITA_ZMQ_ADDR, topics: list[str] | None = None) -> NikitaSubscriber:
    """Return (and lazily create) the singleton NikitaSubscriber."""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = NikitaSubscriber(address=address, topics=topics)
    return _instance
