"""Demo viewer for the customer anonymizer.

Sends synthetic events to Kafka and shows, side by side, each raw event and what the pipeline made of
it: an anonymized output row, a quarantine row, or a DLQ reason code. It also shows the published
profile (k, l, generalization levels, re-identification risk, equivalence classes), the committed
output partitions, the audit log and the stream job's Prometheus metrics.

Demo only: it reads the raw synthetic snapshot and holds the demo HMAC key so it can line up "before"
and "after". Nothing like this belongs inside a real deployment.
"""

import base64
import glob
import hashlib
import hmac
import json
import logging
import os
import random
import threading
import time
import urllib.request
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from statistics import median
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
TOPIC = os.environ.get("SOURCE_TOPIC", "customer-events")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "customer-events-dlq")
DATA = os.environ.get("DATA_DIR", "/data")
METRICS_URL = os.environ.get("METRICS_URL", "http://stream-job:9095/metrics")
TRIGGER = os.environ.get("DEMO_TRIGGER", "15 seconds")
TOKEN_CONTEXT = os.environ.get("TOKEN_CONTEXT", "customer-token").encode()
KEY = base64.b64decode(os.environ["ANON_HMAC_KEY_V1"])
SEED_EVENTS = int(os.environ.get("SEED_EVENTS", "60"))
PORT = int(os.environ.get("PORT", "8080"))
HERE = os.path.dirname(os.path.abspath(__file__))
QI = ["dob", "gender", "zip", "occupation"]
EVENT_TYPES = ["LOGIN", "PAGE_VIEW", "ADD_TO_CART", "PAYMENT", "LOGOUT"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("viewer")


def tokenize(customer_id):
    """Same as Tokenizer.java: hex(HMAC-SHA256(key, context || 0x1F || strip(id)))."""
    msg = TOKEN_CONTEXT + b"\x1f" + customer_id.strip().encode()
    return hmac.new(KEY, msg, hashlib.sha256).hexdigest()


def epoch(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def jsonable(v):
    if isinstance(v, datetime):
        return (v if v.tzinfo else v.replace(tzinfo=timezone.utc)).isoformat().replace("+00:00", "Z")
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


class Snapshot:
    """Raw synthetic customers, loaded once the snapshot step has written them."""

    def __init__(self):
        self.customers = []
        self.by_id = {}

    def ensure(self):
        if self.customers or not os.path.exists(f"{DATA}/snapshot/_SUCCESS"):
            return bool(self.customers)
        rows = pq.read_table(f"{DATA}/snapshot").to_pylist()
        self.customers = rows
        self.by_id = {r["customerId"]: r for r in rows}
        log.info("Loaded snapshot: %d customers", len(rows))
        return True


class CommittedPartitions:
    """Reads only files named in committed batch manifests, as a real consumer must."""

    def __init__(self, root):
        self.root = root
        self.seen = {}  # manifest path -> manifest
        self.rows = []

    def refresh(self):
        for path in sorted(glob.glob(f"{self.root}/dt=*/hr=*/_manifest-b*.json")):
            if path in self.seen:
                continue
            manifest = read_json(path)
            if manifest is None:
                continue
            part_dir = os.path.dirname(path)
            for name in manifest.get("files", []):
                if name.endswith(".parquet"):
                    for r in pq.read_table(os.path.join(part_dir, name)).to_pylist():
                        r["_partition"] = manifest["partition"]
                        r["_batch"] = manifest["batchId"]
                        self.rows.append(r)
            self.seen[path] = manifest
        return self

    def manifests(self):
        return sorted(self.seen.values(), key=lambda m: (m["partition"], m["batchId"]), reverse=True)


class Profile:
    """Latest published profile version plus its equivalence-class statistics."""

    def __init__(self):
        self.version = None
        self.manifest = None
        self.stats = None

    def refresh(self):
        latest_file = f"{DATA}/profile/_LATEST"
        if not os.path.exists(latest_file):
            return self
        with open(latest_file) as f:
            version = f.read().strip()
        if version == self.version:
            return self
        manifest = read_json(f"{DATA}/profile/version={version}/_manifest.json")
        if manifest is None:
            return self
        table = pq.read_table(f"{DATA}/profile/version={version}").to_pylist()
        classes = Counter()
        diversity = {}
        for r in table:
            if r["suppressed"]:
                continue
            key = tuple(r[f"{q}_gen"] for q in QI)
            classes[key] += 1
            diversity.setdefault(key, set()).add(r["incomeBand"])
        sizes = sorted(classes.values())
        buckets = OrderedDict((label, 0) for label in ["10-19", "20-49", "50-99", "100-199", "200-499", "500+"])
        for s in sizes:
            label = ("10-19" if s < 20 else "20-49" if s < 50 else "50-99" if s < 100
                     else "100-199" if s < 200 else "200-499" if s < 500 else "500+")
            buckets[label] += 1
        largest = [{"values": dict(zip(QI, k)), "size": n, "distinctIncome": len(diversity[k])}
                   for k, n in classes.most_common(6)]
        smallest = [{"values": dict(zip(QI, k)), "size": n, "distinctIncome": len(diversity[k])}
                    for k, n in sorted(classes.items(), key=lambda kv: kv[1])[:6]]
        self.stats = {
            "classes": len(sizes),
            "minSize": sizes[0] if sizes else 0,
            "medianSize": median(sizes) if sizes else 0,
            "maxSize": sizes[-1] if sizes else 0,
            "minDistinctSensitive": min((len(v) for v in diversity.values()), default=0),
            "histogram": [{"label": k, "count": v} for k, v in buckets.items()],
            "largest": largest,
            "smallest": smallest,
            "suppressed": sum(1 for r in table if r["suppressed"]),
        }
        self.version, self.manifest = version, manifest
        versions = sorted(os.path.basename(p)[len("version="):] for p in glob.glob(f"{DATA}/profile/version=*"))
        audit = read_json(next(iter(glob.glob(f"{DATA}/audit/dt=*/profile-{version}.json")), "")) or {}
        details = audit.get("details") or {}
        self.manifest = dict(manifest, allVersions=versions,
                             maxProsecutorRisk=details.get("maxProsecutorRisk"),
                             maxAverageRisk=details.get("maxAverageRisk"))
        log.info("Loaded profile version %s (%d rows, %d classes)", version, len(table), len(sizes))
        return self


class TopicTail(threading.Thread):
    """Reads a topic from the beginning and keeps the most recent messages."""

    def __init__(self, topic, keep):
        super().__init__(daemon=True, name=f"tail-{topic}")
        self.topic = topic
        self.messages = deque(maxlen=keep)
        self.total = 0
        self.lock = threading.Lock()

    def run(self):
        while True:
            try:
                self._consume()
            except Exception:  # keep the demo alive across broker restarts
                log.exception("Consumer for %s failed; retrying", self.topic)
                time.sleep(3)

    def _consume(self):
        c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"viewer-{self.topic}-{os.getpid()}",
                      "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        meta = c.list_topics(self.topic, timeout=10).topics[self.topic]
        if not meta.partitions:
            c.close()
            time.sleep(2)
            return
        c.assign([TopicPartition(self.topic, p, OFFSET_BEGINNING) for p in meta.partitions])
        while True:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            with self.lock:
                self.messages.append({"partition": m.partition(), "offset": m.offset(),
                                      "value": m.value().decode("utf-8", "replace") if m.value() else "",
                                      "ts": m.timestamp()[1]})
                self.total += 1

    def snapshot(self):
        with self.lock:
            return list(self.messages), self.total


snapshot = Snapshot()
profile = Profile()
output = CommittedPartitions(f"{DATA}/output")
quarantine = CommittedPartitions(f"{DATA}/quarantine")
source_tail = TopicTail(TOPIC, keep=400)
dlq_tail = TopicTail(DLQ_TOPIC, keep=2000)
producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20})
refresh_lock = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event_for(customer, event_type=None):
    e = {k: customer[k] for k in ["customerId", "firstName", "lastName", "email", "phone"]}
    e.update(dob=jsonable(customer["dob"]), gender=customer["gender"], zip=customer["zip"],
             occupation=customer["occupation"], incomeBand=customer["incomeBand"],
             eventType=event_type or random.choice(EVENT_TYPES), eventTs=now_iso())
    return json.dumps(e)


def make_messages(kind, n):
    if not snapshot.ensure():
        raise RuntimeError("Snapshot not generated yet")
    out = []
    for _ in range(n):
        k = kind
        if kind == "mixed":
            k = random.choices(["good", "unknown", "malformed", "extra_field", "missing_id"],
                               weights=[80, 8, 4, 4, 4])[0]
        c = random.choice(snapshot.customers)
        if k == "good":
            out.append(event_for(c))
        elif k == "unknown":
            stranger = dict(c, customerId=f"N{random.randint(0, 999999):06d}")
            out.append(event_for(stranger))
        elif k == "malformed":
            out.append(event_for(c)[:-15])  # truncated JSON
        elif k == "extra_field":
            out.append(event_for(c)[:-1] + ', "ssn": "123-45-6789"}')
        elif k == "missing_id":
            d = json.loads(event_for(c))
            del d["customerId"]
            out.append(json.dumps(d))
        elif k == "bad":
            out.extend(make_messages(random.choice(["malformed", "extra_field", "missing_id"]), 1))
    return out


def send(kind, n):
    msgs = make_messages(kind, n)
    for m in msgs:
        producer.produce(TOPIC, m.encode())
    producer.flush(10)
    return len(msgs)


def metrics():
    try:
        with urllib.request.urlopen(METRICS_URL, timeout=2) as r:
            text = r.read().decode()
    except Exception:
        return None
    values = {}
    for line in text.splitlines():
        if line.startswith("anon_") and " " in line:
            name, value = line.rsplit(" ", 1)
            base = name.split("{", 1)[0]
            if base == "anon_records_dlq_total":
                reason = name.split('reason="', 1)[1].split('"', 1)[0]
                values.setdefault("dlqByReason", {})[reason] = float(value)
            values[base] = values.get(base, 0) + float(value) if base == "anon_records_dlq_total" else float(value)
    return values


def audit_entries():
    entries = [read_json(p) for p in glob.glob(f"{DATA}/audit/dt=*/*.json")]
    entries = [e for e in entries if e]
    entries.sort(key=lambda e: e.get("committedAt", ""), reverse=True)
    return entries[:25]


def state():
    with refresh_lock:
        snapshot.ensure()
        profile.refresh()
        output.refresh()
        quarantine.refresh()

    out_by_key, out_by_token = {}, {}
    for r in output.rows:
        out_by_key[(r["customer_token"], r["eventType"], epoch(r["eventTs"]))] = r
        out_by_token[r["customer_token"]] = r
    q_by_coord = {(r["kafka_partition"], r["kafka_offset"]): r for r in quarantine.rows}
    dlq_msgs, dlq_total = dlq_tail.snapshot()
    dlq_by_coord = {}
    for m in dlq_msgs:
        try:
            d = json.loads(m["value"])
            dlq_by_coord[(d["sourcePartition"], d["sourceOffset"])] = d
        except (ValueError, KeyError):
            pass

    src_msgs, src_total = source_tail.snapshot()
    events = []
    for m in reversed(src_msgs[-80:]):
        coord = (m["partition"], m["offset"])
        item = {"partition": m["partition"], "offset": m["offset"], "raw": m["value"], "status": "pending"}
        try:
            raw = json.loads(m["value"])
            item["parsed"] = raw if isinstance(raw, dict) else None
        except ValueError:
            raw = None
        if coord in dlq_by_coord:
            item["status"], item["reason"] = "dlq", dlq_by_coord[coord]["reason"]
        elif coord in q_by_coord:
            item["status"], item["result"] = "quarantined", q_by_coord[coord]
        elif isinstance(raw, dict) and raw.get("customerId"):
            token = tokenize(str(raw["customerId"]))
            hit = out_by_key.get((token, raw.get("eventType"), epoch(raw.get("eventTs"))))
            if hit:
                item["status"], item["result"] = "anonymized", hit
        events.append(item)

    return {
        "now": now_iso(),
        "trigger": TRIGGER,
        "topics": {"source": TOPIC, "dlq": DLQ_TOPIC, "sourceTotal": src_total, "dlqTotal": dlq_total},
        "snapshot": {"customers": len(snapshot.customers)},
        "profile": {"manifest": profile.manifest, "stats": profile.stats},
        "metrics": metrics(),
        "events": events,
        "partitions": output.manifests()[:20],
        "quarantinePartitions": quarantine.manifests()[:20],
        "outputRows": len(output.rows),
        "quarantineRows": len(quarantine.rows),
        "dlq": [json.loads(m["value"]) for m in reversed(dlq_msgs[-30:]) if m["value"].startswith("{")],
        "audit": audit_entries(),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body, default=jsonable).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif path == "/api/state":
            try:
                self._send(200, state())
            except Exception as e:
                log.exception("state failed")
                self._send(500, {"error": str(e)})
        elif path == "/health":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if u.path != "/api/send":
            return self._send(404, {"error": "not found"})
        q = parse_qs(u.query)
        kind = q.get("kind", ["mixed"])[0]
        n = max(1, min(500, int(q.get("n", ["20"])[0])))
        if kind not in {"mixed", "good", "unknown", "bad"}:
            return self._send(400, {"error": "unknown kind"})
        try:
            self._send(200, {"sent": send(kind, n), "kind": kind})
        except Exception as e:
            self._send(503, {"error": str(e)})


def seed_when_ready():
    """Send a first mixed batch once, so a fresh demo shows results without any clicks."""
    while not snapshot.ensure():
        time.sleep(2)
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "viewer-seed-check"})
    meta = c.list_topics(TOPIC, timeout=10).topics[TOPIC]
    while not meta.partitions:  # kafka-init has not created the topic yet
        time.sleep(2)
        meta = c.list_topics(TOPIC, timeout=10).topics[TOPIC]
    existing = sum(c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1] for p in meta.partitions)
    c.close()
    if existing == 0 and SEED_EVENTS > 0:
        log.info("Empty source topic: sending %d seed events", SEED_EVENTS)
        send("mixed", SEED_EVENTS)


if __name__ == "__main__":
    source_tail.start()
    dlq_tail.start()
    threading.Thread(target=seed_when_ready, daemon=True, name="seed").start()
    log.info("Viewer on http://0.0.0.0:%d", PORT)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
