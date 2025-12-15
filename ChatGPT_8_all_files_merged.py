# car_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response, HOSTS, yaml_request
import uuid

app = Flask("Car_MS")

# naive car registry for demo
CAR_REGISTRY = {"CAR-ABC": {"status":"idle"}, "CAR-123": {"status":"idle"}}

@app.route("/check_car", methods=["POST"])
def check_car():
    data = parse_yaml_request(request) or {}
    car_id = data.get("car_id")
    # If car_id not present, provide an available car id
    if not car_id:
        # choose available car
        for cid, info in CAR_REGISTRY.items():
            if info.get("status") == "idle":
                return yaml_response({"status":"ok", "car_id": cid})
        # if none available, create one
        new_id = "CAR-" + uuid.uuid4().hex[:6]
        CAR_REGISTRY[new_id] = {"status":"idle"}
        return yaml_response({"status":"ok", "car_id": new_id})
    else:
        # check known
        ok = car_id in CAR_REGISTRY
        return yaml_response({"status":"ok", "exists": ok, "car_id": car_id})

@app.route("/notify_assignment", methods=["POST"])
def notify_assignment():
    data = parse_yaml_request(request) or {}
    delivery_id = data.get("delivery_id")
    # acknowledge
    # We might update car status:
    car_id = data.get("car_id")
    if car_id:
        CAR_REGISTRY.setdefault(car_id, {})['status'] = 'assigned'
    return yaml_response({"status":"ok", "ack": True, "delivery_id": delivery_id})

@app.route("/request_update", methods=["POST"])
def request_update():
    data = parse_yaml_request(request) or {}
    # Car requests delivery update (e.g. change status)
    # forward to Controller_MS
    controller_url = f"{HOSTS['Controller_MS']}/car_request_update"
    try:
        res = yaml_request(controller_url, data)
        return yaml_response({"status":"ok", "controller_response": res})
    except Exception as e:
        return yaml_response({"status":"error", "error": str(e)}, status=500)

if __name__ == "__main__":
    app.run(port=5006)

# common.py
import yaml
import requests
from flask import request, Response
import os

TIMEOUT = 5  # seconds for service-to-service calls

def yaml_request(url, payload):
    """
    Send YAML payload and return parsed YAML or raise.
    """
    headers = {"Content-Type": "application/x-yaml", "Accept": "application/x-yaml"}
    data = yaml.safe_dump(payload)
    resp = requests.post(url, data=data.encode("utf-8"), headers=headers, timeout=TIMEOUT)
    resp.raise_for_status()
    if resp.content:
        return yaml.safe_load(resp.content)
    return None

def parse_yaml_request(flask_request):
    """
    Parse incoming YAML request body to Python object.
    """
    if not flask_request.data:
        return None
    return yaml.safe_load(flask_request.data)

def yaml_response(obj, status=200):
    """
    Return a Flask Response with YAML body.
    """
    data = yaml.safe_dump(obj or {})
    return Response(data, status=status, mimetype="application/x-yaml")

# Host configuration (change when deploying on different machines)
HOSTS = {
    "Sender_MS": os.environ.get("SENDER_HOST", "http://localhost:5001"),
    "UI_MS": os.environ.get("UI_HOST", "http://localhost:5002"),
    "IDGen_MS": os.environ.get("IDGEN_HOST", "http://localhost:5003"),
    "Controller_MS": os.environ.get("CONTROLLER_HOST", "http://localhost:5004"),
    "Storage_MS": os.environ.get("STORAGE_HOST", "http://localhost:5005"),
    "Car_MS": os.environ.get("CAR_HOST", "http://localhost:5006"),
    "Log_MS": os.environ.get("LOG_HOST", "http://localhost:5007"),
}

# controller_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response, HOSTS, yaml_request
import uuid

app = Flask("Controller_MS")

# Helpers to log via Log_MS
def log(message, source="Controller_MS"):
    log_url = f"{HOSTS['Log_MS']}/log"
    payload = {"source": source, "message": message}
    try:
        yaml_request(log_url, payload)
    except Exception:
        pass  # best-effort logging

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    data = parse_yaml_request(request) or {}
    # Flow:
    # - Request parcel ID from IDGen_MS
    # - Request car ID from Car_MS
    # - Save both via Storage_MS, then assign delivery, store in Database_1
    # - Notify Car_MS and UI_MS and Sender_MS through UI
    sender = data.get("sender")
    # 1) parcel id
    idgen_url = f"{HOSTS['IDGen_MS']}/generate"
    parcel_resp = yaml_request(idgen_url, {"requestor":"controller"})
    parcel_id = parcel_resp.get("parcel_id")

    # log
    log(f"Generated parcel_id {parcel_id}")

    # 2) get car id
    car_url = f"{HOSTS['Car_MS']}/check_car"
    car_resp = yaml_request(car_url, {})
    car_id = car_resp.get("car_id")

    log(f"Assigned car_id {car_id}")

    # Store both via Storage_MS
    storage_store_id = f"{HOSTS['Storage_MS']}/store_id"
    yaml_request(storage_store_id, {"id_key": f"parcel:{parcel_id}", "id_value": parcel_id})
    yaml_request(storage_store_id, {"id_key": f"car:{car_id}", "id_value": car_id})

    # Confirm retrieval from Storage
    # (not strictly necessary, but following your flow)
    parcel_from_storage = yaml_request(f"{HOSTS['Storage_MS']}/get_id", {"id_key": f"parcel:{parcel_id}"})
    car_from_storage = yaml_request(f"{HOSTS['Storage_MS']}/get_id", {"id_key": f"car:{car_id}"})

    # Assign delivery
    delivery_id = "D-" + uuid.uuid4().hex[:10]
    # store delivery
    yaml_request(f"{HOSTS['Storage_MS']}/store_delivery", {
        "delivery_id": delivery_id,
        "parcel_id": parcel_id,
        "car_id": car_id,
        "metadata": {"sender": sender}
    })

    log(f"Created delivery {delivery_id} for parcel {parcel_id} with car {car_id}")

    # Notify Car_MS
    yaml_request(f"{HOSTS['Car_MS']}/notify_assignment", {"delivery_id": delivery_id, "car_id": car_id})
    log(f"Notified car {car_id} of delivery {delivery_id}")

    # Notify UI -> UI will notify Sender
    yaml_request(f"{HOSTS['UI_MS']}/notify_sender", {"delivery_id": delivery_id, "parcel_id": parcel_id, "car_id": car_id, "sender": sender})
    log(f"Notified UI about delivery {delivery_id}")

    return yaml_response({"status":"ok", "delivery_id": delivery_id, "parcel_id": parcel_id, "car_id": car_id})

@app.route("/car_request_update", methods=["POST"])
def car_request_update():
    data = parse_yaml_request(request) or {}
    # Car requested update (e.g., change status). Acknowledge, then update Storage.
    car_id = data.get("car_id")
    delivery_id = data.get("delivery_id")
    new_status = data.get("status", "in_transit")
    # Acknowledge car first
    # Update storage
    yaml_request(f"{HOSTS['Storage_MS']}/update_delivery", {"delivery_id": delivery_id, "status": new_status})
    log(f"Car {car_id} updated delivery {delivery_id} -> {new_status}")
    # Notify UI -> UI will notify Sender
    yaml_request(f"{HOSTS['UI_MS']}/notify_update", {"delivery_id": delivery_id, "status": new_status})
    return yaml_response({"status":"ok", "ack": True})

if __name__ == "__main__":
    app.run(port=5004)

# db_init.py
import sqlite3

def init():
    # Database 1: Database_1 -> parcels & deliveries
    conn1 = sqlite3.connect('db_parcels.sqlite')
    c1 = conn1.cursor()
    c1.execute('''
        CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id TEXT PRIMARY KEY,
            parcel_id TEXT,
            car_id TEXT,
            status TEXT,
            metadata TEXT
        )
    ''')
    conn1.commit(); conn1.close()

    # Database 2: Database_2 -> assignments and stored ids
    conn2 = sqlite3.connect('db_assignments.sqlite')
    c2 = conn2.cursor()
    c2.execute('''
        CREATE TABLE IF NOT EXISTS ids (
            id_key TEXT PRIMARY KEY,
            id_value TEXT
        )
    ''')
    conn2.commit(); conn2.close()

    # Database 3: Database_3 -> logs
    conn3 = sqlite3.connect('db_logs.sqlite')
    c3 = conn3.cursor()
    c3.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            message TEXT
        )
    ''')
    conn3.commit(); conn3.close()

if __name__ == "__main__":
    init()
    print("Databases initialized.")

# idgen_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response, HOSTS, yaml_request
import uuid

app = Flask("IDGen_MS")

@app.route("/generate", methods=["POST"])
def generate():
    data = parse_yaml_request(request) or {}
    # Generate parcel ID
    parcel_id = "P-" + uuid.uuid4().hex[:12]
    # Share with Storage_MS
    payload = {"action": "store_id", "id_key": f"parcel:{parcel_id}", "id_value": parcel_id}
    storage_url = f"{HOSTS['Storage_MS']}/store_id"
    try:
        ack = yaml_request(storage_url, payload)
    except Exception as e:
        return yaml_response({"status": "error", "error": str(e)}, status=500)
    # Acknowledge Controller (caller)
    return yaml_response({"status": "ok", "parcel_id": parcel_id})

if __name__ == "__main__":
    app.run(port=5003)

# log_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response
import sqlite3

app = Flask("Log_MS")

DB = 'db_logs.sqlite'  # Database_3

def store_log(source, message):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("INSERT INTO logs (source, message) VALUES (?, ?)", (source, message))
    conn.commit()
    conn.close()

@app.route("/log", methods=["POST"])
def log():
    data = parse_yaml_request(request) or {}
    source = data.get("source")
    message = data.get("message")
    store_log(source or "unknown", str(message))
    return yaml_response({"status": "ok", "stored": True})

if __name__ == "__main__":
    app.run(port=5007)

# sender_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response, HOSTS, yaml_request

app = Flask("Sender_MS")

@app.route("/start_request", methods=["POST"])
def start_request():
    """
    Sender initiates request_delivery -> UI_MS
    """
    data = parse_yaml_request(request) or {}
    ui_url = f"{HOSTS['UI_MS']}/request_delivery_from_sender"
    try:
        res = yaml_request(ui_url, {"sender": data.get("sender", "unknown"), "items": data.get("items", [])})
        return yaml_response({"status":"ok", "ui_response": res})
    except Exception as e:
        return yaml_response({"status":"error", "error": str(e)}, status=500)

@app.route("/notify", methods=["POST"])
def notify():
    data = parse_yaml_request(request) or {}
    # Acknowledge
    return yaml_response({"status":"ok", "received": True, "details": data})

if __name__ == "__main__":
    app.run(port=5001)

# storage_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response
import sqlite3
import json

app = Flask("Storage_MS")
DB_ASSIGN = 'db_assignments.sqlite'  # Database_2
DB_PARCELS = 'db_parcels.sqlite'     # Database_1

def store_assignment(id_key, id_value):
    conn = sqlite3.connect(DB_ASSIGN)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO ids (id_key, id_value) VALUES (?, ?)", (id_key, id_value))
    conn.commit(); conn.close()

def get_assignment(id_key):
    conn = sqlite3.connect(DB_ASSIGN)
    c = conn.cursor()
    c.execute("SELECT id_value FROM ids WHERE id_key=?", (id_key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def store_delivery(delivery_id, parcel_id, car_id, status="assigned", metadata=None):
    conn = sqlite3.connect(DB_PARCELS)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO deliveries (delivery_id, parcel_id, car_id, status, metadata) VALUES (?, ?, ?, ?, ?)",
              (delivery_id, parcel_id, car_id, status, json.dumps(metadata or {})))
    conn.commit(); conn.close()

def update_delivery(delivery_id, **fields):
    conn = sqlite3.connect(DB_PARCELS)
    c = conn.cursor()
    # naive update (only status and metadata and car_id and parcel_id handled)
    if 'status' in fields:
        c.execute("UPDATE deliveries SET status=? WHERE delivery_id=?", (fields['status'], delivery_id))
    if 'metadata' in fields:
        c.execute("UPDATE deliveries SET metadata=? WHERE delivery_id=?", (json.dumps(fields['metadata']), delivery_id))
    if 'car_id' in fields:
        c.execute("UPDATE deliveries SET car_id=? WHERE delivery_id=?", (fields['car_id'], delivery_id))
    if 'parcel_id' in fields:
        c.execute("UPDATE deliveries SET parcel_id=? WHERE delivery_id=?", (fields['parcel_id'], delivery_id))
    conn.commit(); conn.close()

@app.route("/store_id", methods=["POST"])
def store_id():
    data = parse_yaml_request(request) or {}
    id_key = data.get("id_key")
    id_value = data.get("id_value")
    if not id_key or not id_value:
        return yaml_response({"status": "error", "error": "missing id_key or id_value"}, status=400)
    store_assignment(id_key, id_value)
    return yaml_response({"status": "ok", "stored": True})

@app.route("/get_id", methods=["POST"])
def get_id():
    data = parse_yaml_request(request) or {}
    id_key = data.get("id_key")
    if not id_key:
        return yaml_response({"status": "error", "error": "missing id_key"}, status=400)
    value = get_assignment(id_key)
    return yaml_response({"status": "ok", "id_value": value})

@app.route("/store_delivery", methods=["POST"])
def store_delivery_endpoint():
    data = parse_yaml_request(request) or {}
    delivery_id = data.get("delivery_id")
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    metadata = data.get("metadata")
    if not delivery_id or not parcel_id or not car_id:
        return yaml_response({"status": "error", "error": "missing fields"}, status=400)
    store_delivery(delivery_id, parcel_id, car_id, metadata=metadata)
    return yaml_response({"status": "ok", "stored": True})

@app.route("/update_delivery", methods=["POST"])
def update_delivery_endpoint():
    data = parse_yaml_request(request) or {}
    delivery_id = data.get("delivery_id")
    fields = {}
    for k in ("status","metadata","car_id","parcel_id"):
        if k in data:
            fields[k] = data[k]
    if not delivery_id:
        return yaml_response({"status": "error", "error": "missing delivery_id"}, status=400)
    update_delivery(delivery_id, **fields)
    return yaml_response({"status": "ok", "updated": True})

if __name__ == "__main__":
    app.run(port=5005)

# ui_ms.py
from flask import Flask, request
from common import parse_yaml_request, yaml_response, HOSTS, yaml_request

app = Flask("UI_MS")

@app.route("/request_delivery_from_sender", methods=["POST"])
def request_delivery_from_sender():
    data = parse_yaml_request(request) or {}
    # Sender_MS -> UI_MS => forward to Controller_MS
    controller_url = f"{HOSTS['Controller_MS']}/request_delivery"
    try:
        resp = yaml_request(controller_url, data)
        return yaml_response({"status":"ok", "controller_response": resp})
    except Exception as e:
        return yaml_response({"status":"error", "error": str(e)}, status=500)

@app.route("/notify_sender", methods=["POST"])
def notify_sender():
    data = parse_yaml_request(request) or {}
    # UI notifies Sender_MS (forward)
    sender_url = f"{HOSTS['Sender_MS']}/notify"
    try:
        yaml_request(sender_url, data)
    except Exception:
        pass
    # UI acknowledges Controller_MS (if Controller expects ack)
    return yaml_response({"status":"ok", "notified_sender": True})

@app.route("/notify_update", methods=["POST"])
def notify_update():
    data = parse_yaml_request(request) or {}
    # Notify Sender about delivery update
    sender_url = f"{HOSTS['Sender_MS']}/notify"
    try:
        yaml_request(sender_url, data)
    except Exception:
        pass
    return yaml_response({"status":"ok"})

if __name__ == "__main__":
    app.run(port=5002)

