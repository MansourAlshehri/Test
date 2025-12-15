# car_ms.py
from fastapi import FastAPI, Request, Response
import yaml
import requests
import datetime

app = FastAPI(title="Car_MS")

STORAGE_URL = "http://localhost:8004/store_id"
LOG_URL = "http://localhost:8006/log"

def send_yaml(url, payload):
    raw = yaml.safe_dump(payload)
    return requests.post(url, data=raw.encode(), headers={"Content-Type": "application/x-yaml"})

@app.post("/check_car")
async def check_car(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    car_id = data.get("car_id")
    if not car_id:
        return Response(yaml.safe_dump({"status": "error", "reason": "missing car_id"}), media_type="application/x-yaml", status_code=400)
    # Simulate check
    ok = True
    # share with storage
    send_yaml(STORAGE_URL, {"type": "car", "id": car_id})
    # log
    log = {"timestamp": datetime.datetime.utcnow().isoformat(), "source": "Car_MS", "message": f"car {car_id} checked and shared with Storage_MS"}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(log).encode(), headers={"Content-Type": "application/x-yaml"})
    except Exception:
        pass
    return Response(yaml.safe_dump({"status": "ok", "car_id": car_id, "available": ok}), media_type="application/x-yaml")

@app.post("/notify")
async def notify(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Pretend to accept notification
    # Acknowledge
    return Response(yaml.safe_dump({"status": "ack"}), media_type="application/x-yaml")

@app.post("/request_update")
async def request_update(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Car requests delivery update; forward to Controller by calling Controller endpoint
    controller_url = "http://localhost:8000/car_update_request"
    resp = requests.post(controller_url, data=yaml.safe_dump({"car_id": data.get("car_id")}).encode(),
                         headers={"Content-Type": "application/x-yaml"})
    return Response(yaml.safe_dump({"controller_response": resp.text if resp is not None else ""}), media_type="application/x-yaml")

# controller_ms.py
from fastapi import FastAPI, Request, Response
import yaml
import requests
import datetime

app = FastAPI(title="Controller_MS")

IDGEN_URL = "http://localhost:8003/request_id"
STORAGE_URL = "http://localhost:8004"
CAR_URL = "http://localhost:8005"
UI_URL = "http://localhost:8001"
LOG_URL = "http://localhost:8006/log"

def send_yaml(url, payload):
    raw = yaml.safe_dump(payload)
    return requests.post(url, data=raw.encode(), headers={"Content-Type": "application/x-yaml"})

def log(msg):
    payload = {"timestamp": datetime.datetime.utcnow().isoformat(), "source": "Controller_MS", "message": msg}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(payload).encode(), headers={"Content-Type": "application/x-yaml"})
    except Exception:
        pass

@app.post("/handle_request_delivery")
async def handle_request_delivery(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Step: request parcel ID from IDGen_MS
    id_resp = send_yaml(IDGEN_URL, {"request": "parcel_id"})
    # id_resp contains YAML text; parse
    try:
        id_data = yaml.safe_load(id_resp.text)
        parcel_id = id_data.get("parcel_id")
    except Exception:
        parcel_id = None
    log(f"requested parcel id, got {parcel_id}")

    # Request car id from Car_MS (controller asks Car_MS)
    car_check = send_yaml(f"{CAR_URL}/check_car", {"car_id": data.get("preferred_car", "CAR-001")})
    try:
        car_data = yaml.safe_load(car_check.text)
        car_id = car_data.get("car_id")
    except Exception:
        car_id = None
    log(f"requested car id, got {car_id}")

    # Fetch parcel id and car id from Storage to confirm (per flow)
    s_parcel = send_yaml(f"{STORAGE_URL}/get_ids", {"type": "parcel"})
    s_car = send_yaml(f"{STORAGE_URL}/get_ids", {"type": "car"})
    # Assign delivery
    delivery = {"parcel_id": parcel_id, "car_id": car_id, "status": "assigned", "content": data.get("content","")}
    # Share delivery with Storage_MS
    store_resp = send_yaml(f"{STORAGE_URL}/store_delivery", delivery)
    log(f"stored delivery: {delivery}")

    # Notify Car_MS
    send_yaml(f"{CAR_URL}/notify", {"parcel_id": parcel_id, "car_id": car_id, "action": "new_assignment"})
    log("notified Car_MS")

    # Notify UI_MS
    send_yaml(f"{UI_URL}/notify_ui", {"parcel_id": parcel_id, "car_id": car_id, "status": "assigned"})
    log("notified UI_MS")

    return Response(yaml.safe_dump({"status": "delivery_assigned", "parcel_id": parcel_id, "car_id": car_id}), media_type="application/x-yaml")

@app.post("/car_update_request")
async def car_update_request(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    car_id = data.get("car_id")
    # Acknowledge Car_MS
    # Controller shares update with Storage_MS (Controller_MS shares delivery update with Storage_MS)
    # For this sample, we fetch assignments and update status
    # We will pretend to update a parcel assigned to this car
    # Query assignments table (Storage has assignment mapping) - call Storage get_ids
    resp = send_yaml(f"{STORAGE_URL}/get_ids", {"type": "assign"})
    # Simpler: we will directly tell Storage_MS to update a parcel (demo)
    update_payload = {"parcel_id": data.get("parcel_id","PKG-unknown"), "car_id": car_id, "status": "in_transit"}
    send_yaml(f"{STORAGE_URL}/store_delivery", update_payload)
    # Notify UI
    send_yaml("http://localhost:8001/notify_ui", {"parcel_id": update_payload["parcel_id"], "car_id": car_id, "status": "in_transit"})
    log(f"processed car update for {car_id}")
    return Response(yaml.safe_dump({"status": "ack"}), media_type="application/x-yaml")

# idgen_ms.py
from fastapi import FastAPI, Request, Response
import yaml
import requests
import uuid
import datetime

app = FastAPI(title="IDGen_MS")

STORAGE_URL = "http://localhost:8004/store_id"  # Storage_MS
LOG_URL = "http://localhost:8006/log"          # Log_MS

def send_yaml(url, payload):
    raw = yaml.safe_dump(payload)
    return requests.post(url, data=raw.encode(), headers={"Content-Type": "application/x-yaml"})

@app.post("/request_id")
async def request_id(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or "{}")
    # generate ID
    parcel_id = f"PKG-{uuid.uuid4().hex[:12]}"
    # share with Storage_MS
    payload = {"type": "parcel", "id": parcel_id}
    resp = send_yaml(STORAGE_URL, payload)
    # log the action to Log_MS
    log = {"timestamp": datetime.datetime.utcnow().isoformat(), "source": "IDGen_MS", "message": f"generated {parcel_id} and shared with Storage_MS"}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(log).encode(), headers={"Content-Type": "application/x-yaml"})
    except Exception:
        pass
    return Response(yaml.safe_dump({"parcel_id": parcel_id, "storage_response": resp.text if resp is not None else ""}), media_type="application/x-yaml")

# log_ms.py
from fastapi import FastAPI, Request, Response
import sqlite3
import yaml
import datetime

app = FastAPI(title="Log_MS")

DB = "db_logs.db"

def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        source TEXT,
        message TEXT
      )
    """)
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()

@app.post("/log")
async def receive_log(request: Request):
    raw = await request.body()
    payload = yaml.safe_load(raw.decode() or "{}")
    timestamp = payload.get("timestamp") or datetime.datetime.utcnow().isoformat()
    source = payload.get("source", "unknown")
    message = payload.get("message", "")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("INSERT INTO logs (timestamp, source, message) VALUES (?, ?, ?)",
                (timestamp, source, message))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status": "ok"}), media_type="application/x-yaml")

# sender_ms.py
from fastapi import FastAPI, Request, Response
import yaml
import requests
import datetime

app = FastAPI(title="Sender_MS")
UI_URL = "http://localhost:8001/request_delivery"
LOG_URL = "http://localhost:8006/log"

def send_yaml(url, payload):
    raw = yaml.safe_dump(payload)
    return requests.post(url, data=raw.encode(), headers={"Content-Type": "application/x-yaml"})

@app.post("/send_delivery_request")
async def send_delivery_request(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Send request to UI_MS
    resp = send_yaml(UI_URL, data)
    return Response(resp.text if resp is not None else yaml.safe_dump({"error":"no response"}), media_type="application/x-yaml")

@app.post("/notify_sender")
async def notify_sender(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Acknowledge UI
    return Response(yaml.safe_dump({"status": "ack"}), media_type="application/x-yaml")

@app.post("/ack_from_ui")
async def ack_from_ui(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Acknowledge Controller through UI if required; this endpoint just exists for UI -> Sender ack flow
    return Response(yaml.safe_dump({"status": "sender_acknowledged"}), media_type="application/x-yaml")

# storage_ms.py
from fastapi import FastAPI, Request, Response
import sqlite3
import yaml
import datetime

app = FastAPI(title="Storage_MS")
DB_PARCELS = "db_parcels.db"     # Database_1
DB_ASSIGN = "db_assignments.db"  # Database_2

def init_db():
    conn1 = sqlite3.connect(DB_PARCELS)
    cur1 = conn1.cursor()
    cur1.execute("""
      CREATE TABLE IF NOT EXISTS parcels (
        parcel_id TEXT PRIMARY KEY,
        content TEXT,
        status TEXT,
        updated_at TEXT
      )
    """)
    conn1.commit()
    conn1.close()

    conn2 = sqlite3.connect(DB_ASSIGN)
    cur2 = conn2.cursor()
    cur2.execute("""
      CREATE TABLE IF NOT EXISTS assignments (
        key TEXT PRIMARY KEY,
        value TEXT,
        created_at TEXT
      )
    """)
    conn2.commit()
    conn2.close()

@app.on_event("startup")
def startup():
    init_db()

@app.post("/store_id")
async def store_id(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or "{}")
    # data expected: { "type": "parcel"|"car", "id": "..." }
    typ = data.get("type")
    ident = data.get("id")
    created = datetime.datetime.utcnow().isoformat()
    if not typ or not ident:
        return Response(yaml.safe_dump({"status": "error", "reason": "missing type or id"}), media_type="application/x-yaml", status_code=400)
    conn = sqlite3.connect(DB_ASSIGN)
    cur = conn.cursor()
    key = f"{typ}:{ident}"
    cur.execute("INSERT OR REPLACE INTO assignments (key, value, created_at) VALUES (?, ?, ?)",
                (key, ident, created))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status": "ok", "key": key}), media_type="application/x-yaml")

@app.post("/get_ids")
async def get_ids(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or "{}")
    # optional filter by type
    typ = data.get("type")
    conn = sqlite3.connect(DB_ASSIGN)
    cur = conn.cursor()
    if typ:
        cur.execute("SELECT value FROM assignments WHERE key LIKE ?", (f"{typ}:%",))
    else:
        cur.execute("SELECT value FROM assignments")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return Response(yaml.safe_dump({"ids": rows}), media_type="application/x-yaml")

@app.post("/store_delivery")
async def store_delivery(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or "{}")
    # expected: { "parcel_id": "...", "car_id": "...", "status": "...", "content": "..." }
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    status = data.get("status", "assigned")
    content = data.get("content", "")
    now = datetime.datetime.utcnow().isoformat()
    if not parcel_id:
        return Response(yaml.safe_dump({"status": "error", "reason": "missing parcel_id"}), media_type="application/x-yaml", status_code=400)
    conn = sqlite3.connect(DB_PARCELS)
    cur = conn.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO parcels (parcel_id, content, status, updated_at)
      VALUES (?, ?, ?, ?)
    """, (parcel_id, content or f"assigned to {car_id}", status, now))
    conn.commit()
    conn.close()
    # Also store assignment mapping in assignments DB for lookup
    if car_id:
        conn2 = sqlite3.connect(DB_ASSIGN)
        cur2 = conn2.cursor()
        cur2.execute("INSERT OR REPLACE INTO assignments (key, value, created_at) VALUES (?, ?, ?)",
                     (f"assign:{parcel_id}", car_id, now))
        conn2.commit()
        conn2.close()
    return Response(yaml.safe_dump({"status": "ok"}), media_type="application/x-yaml")

@app.post("/get_delivery")
async def get_delivery(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or "{}")
    parcel_id = data.get("parcel_id")
    conn = sqlite3.connect(DB_PARCELS)
    cur = conn.cursor()
    if parcel_id:
        cur.execute("SELECT parcel_id, content, status, updated_at FROM parcels WHERE parcel_id = ?", (parcel_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return Response(yaml.safe_dump({"parcel": {"parcel_id": row[0], "content": row[1], "status": row[2], "updated_at": row[3]}}), media_type="application/x-yaml")
        else:
            return Response(yaml.safe_dump({"status": "not_found"}), media_type="application/x-yaml", status_code=404)
    else:
        cur.execute("SELECT parcel_id, content, status, updated_at FROM parcels")
        rows = [{"parcel_id": r[0], "content": r[1], "status": r[2], "updated_at": r[3]} for r in cur.fetchall()]
        conn.close()
        return Response(yaml.safe_dump({"parcels": rows}), media_type="application/x-yaml")

# ui_ms.py
from fastapi import FastAPI, Request, Response
import yaml
import requests
import datetime

app = FastAPI(title="UI_MS")
CONTROLLER_URL = "http://localhost:8000/handle_request_delivery"
SENDER_ACK_URL = "http://localhost:8002/ack_from_ui"
LOG_URL = "http://localhost:8006/log"

def send_yaml(url, payload):
    raw = yaml.safe_dump(payload)
    return requests.post(url, data=raw.encode(), headers={"Content-Type": "application/x-yaml"})

def log(msg):
    payload = {"timestamp": datetime.datetime.utcnow().isoformat(), "source": "UI_MS", "message": msg}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(payload).encode(), headers={"Content-Type": "application/x-yaml"})
    except Exception:
        pass

@app.post("/request_delivery")
async def request_delivery(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Forward to Controller_MS
    resp = send_yaml(CONTROLLER_URL, data)
    # ack Sender_MS (simulate immediate ack)
    send_yaml(SENDER_ACK_URL, {"status": "received"})
    log("forwarded request_delivery to Controller_MS and acknowledged Sender_MS")
    return Response(resp.text, media_type="application/x-yaml")

@app.post("/notify_ui")
async def notify_ui(request: Request):
    raw = await request.body()
    data = yaml.safe_load(raw.decode() or {})
    # Forward to sender
    # Forwards to Sender_MS
    try:
        requests.post("http://localhost:8002/notify_sender", data=yaml.safe_dump(data).encode(), headers={"Content-Type": "application/x-yaml"})
    except Exception:
        pass
    log(f"notified sender: {data}")
    return Response(yaml.safe_dump({"status": "ok"}), media_type="application/x-yaml")

