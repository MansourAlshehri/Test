#(Car_MS — checks car ID and acknowledges)
# 'Update car_ms.py to include a notify_delivery endpoint:' is generated after controller_ms.py
from flask import Flask, request
from utils import parse_yaml_request, make_yaml_response
import yaml, requests
import sqlite3, os

app = Flask("Car_MS")

# Storage endpoint to save car info
STORAGE_URL = "http://localhost:5005/store_car"

# Example in-memory accepted cars list (in real system would be separate DB)
ACCEPTED_CARS = {"CAR-100","CAR-200","CAR-300"}

@app.route("/check_car", methods=["POST"])
def check_car():
    body = parse_yaml_request(request)
    car_id = body.get("car_id")
    if car_id is None:
        return make_yaml_response({"status":"ERROR","message":"no car_id in request"}, status=400)
    # check availability - for example accept if in ACCEPTED_CARS
    ok = car_id in ACCEPTED_CARS
    # share car id with Storage_MS regardless (per flow)
    payload = {"car_id": car_id, "available": ok}
    try:
        requests.post(STORAGE_URL, data=yaml.safe_dump(payload), headers={"Content-Type":"application/x-yaml"}, timeout=5)
    except Exception as e:
        # still return acknowledgment but note storage issue
        return make_yaml_response({"status":"ERROR","message":"storage failed", "detail": str(e)}, status=500)
    # respond to controller
    return make_yaml_response({"status":"OK","car_id":car_id, "available": ok})

# add to car_ms.py (below /check_car)
@app.route("/notify_delivery", methods=["POST"])
def notify_delivery():
    body = parse_yaml_request(request)
    # Car receives notification of assigned delivery
    # For demo, just acknowledge to Controller_MS by returning OK
    # In real life would trigger driver UI, etc.
    return make_yaml_response({"status":"OK","message":"car notified", "received": body})


if __name__ == "__main__":
    app.run(port=5006, host="0.0.0.0")

#(Controller_MS — orchestrator)
from flask import Flask, request
import requests, yaml
from utils import parse_yaml_request, make_yaml_response

app = Flask("Controller_MS")

IDGEN_URL = "http://localhost:5003/generate_id"
LOG_URL = "http://localhost:5007/log"
CAR_CHECK_URL = "http://localhost:5006/check_car"
STORAGE_STORE_DELIVERY = "http://localhost:5005/store_delivery"
STORAGE_GET_PARCEL = "http://localhost:5005/get_parcel"
STORAGE_GET_CAR = "http://localhost:5005/get_car"
STORAGE_UPDATE = "http://localhost:5005/update_delivery"

# Helper to post log
def send_log(source, event, details):
    payload = {"source": source, "event": event, "details": details}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(payload), headers={"Content-Type":"application/x-yaml"}, timeout=3)
    except Exception:
        pass  # keep controller robust

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    body = parse_yaml_request(request)
    # Step: request parcel ID from IDGen_MS
    send_log("Controller_MS", "requesting_parcel_id", {"request_body": body})
    r = requests.post(IDGEN_URL, data=yaml.safe_dump(body), headers={"Content-Type":"application/x-yaml"})
    idgen_resp = yaml.safe_load(r.text)
    parcel_id = idgen_resp.get("parcel_id")
    send_log("Controller_MS", "parcel_id_received", {"parcel_id": parcel_id})
    # Step: request car id from Car_MS (for simplicity, client provides preferred car_id in request)
    requested_car = body.get("preferred_car", "CAR-100")
    send_log("Controller_MS", "requesting_car_id", {"requested_car": requested_car})
    r2 = requests.post(CAR_CHECK_URL, data=yaml.safe_dump({"car_id": requested_car}), headers={"Content-Type":"application/x-yaml"})
    car_resp = yaml.safe_load(r2.text)
    car_id = car_resp.get("car_id")
    send_log("Controller_MS", "car_id_received", {"car_id": car_id, "available": car_resp.get("available")})
    # Step: read parcel & car from Storage_MS (per flow)
    # (Storage_MS stored them earlier in IDGen and Car steps)
    # assign delivery
    delivery = {
        "parcel_id": parcel_id,
        "car_id": car_id,
        "delivery": {
            "parcel_data": body.get("parcel", {}),
            "status": "assigned",
            "meta": {"notes":"assigned by Controller"}
        }
    }
    # share delivery with Storage_MS
    send_log("Controller_MS", "storing_delivery", {"parcel_id": parcel_id, "car_id": car_id})
    requests.post(STORAGE_STORE_DELIVERY, data=yaml.safe_dump(delivery), headers={"Content-Type":"application/x-yaml"})
    send_log("Controller_MS", "delivery_stored", {"parcel_id": parcel_id})
    # Notify Car_MS (Controller-MS notifies Car_MS)
    try:
        requests.post(f"http://localhost:5006/notify_delivery", data=yaml.safe_dump({"parcel_id":parcel_id,"car_id":car_id}), headers={"Content-Type":"application/x-yaml"}, timeout=3)
    except Exception:
        pass
    send_log("Controller_MS", "notified_car", {"car_id": car_id})
    # Notify UI_MS
    try:
        requests.post("http://localhost:5002/notify_delivery", data=yaml.safe_dump({"parcel_id":parcel_id,"car_id":car_id}), headers={"Content-Type":"application/x-yaml"}, timeout=3)
    except Exception:
        pass
    send_log("Controller_MS", "notified_ui", {"parcel_id": parcel_id})
    return make_yaml_response({"status":"OK","parcel_id":parcel_id,"car_id":car_id})

@app.route("/delivery_update_request", methods=["POST"])
def delivery_update_request():
    body = parse_yaml_request(request)
    # Called e.g. by Car_MS to get updates / ack
    # Acknowledge Car_MS
    send_log("Controller_MS","car_requested_update", body)
    # share update with Storage_MS
    update_payload = {"parcel_id": body.get("parcel_id"), "updates": body.get("updates", {})}
    requests.post("http://localhost:5005/update_delivery", data=yaml.safe_dump(update_payload), headers={"Content-Type":"application/x-yaml"})
    send_log("Controller_MS","storage_updated", {"parcel_id": body.get("parcel_id")})
    # notify UI_MS
    try:
        requests.post("http://localhost:5002/notify_update", data=yaml.safe_dump({"parcel_id":body.get("parcel_id"), "updates": body.get("updates",{})}), headers={"Content-Type":"application/x-yaml"}, timeout=3)
    except Exception:
        pass
    send_log("Controller_MS","ui_notified", {"parcel_id": body.get("parcel_id")})
    return make_yaml_response({"status":"OK","message":"update processed"})

if __name__ == "__main__":
    app.run(port=5004, host="0.0.0.0")

#(IDGen_MS — generates parcel IDs and acknowledges)
from flask import Flask, request
import uuid, requests
from utils import parse_yaml_request, make_yaml_response
import yaml

app = Flask("IDGen_MS")

# configurable endpoints (Storage_MS)
STORAGE_URL = "http://localhost:5005/store_id"
CONTROLLER_ACK = None  # Controller will call back with acknowledgement endpoint in headers/body if needed

@app.route("/generate_id", methods=["POST"])
def generate_id():
    body = parse_yaml_request(request)
    # generate unique parcel id
    parcel_id = f"PARCEL-{uuid.uuid4().hex[:12]}"
    # share parcel id with Storage_MS
    data_to_storage = {"parcel_id": parcel_id, "original_request": body}
    try:
        resp = requests.post(STORAGE_URL, data=yaml.safe_dump(data_to_storage), headers={"Content-Type":"application/x-yaml"}, timeout=5)
        # Storage_MS acknowledges
    except Exception as e:
        return make_yaml_response({"status":"ERROR","message":str(e)}, status=500)
    # acknowledge controller (return parcel_id)
    return make_yaml_response({"status":"OK","parcel_id": parcel_id})

if __name__ == "__main__":
    app.run(port=5003, host="0.0.0.0")

#(Log_MS — writes logs into Database_3)
from flask import Flask, request
import sqlite3, os, yaml
from utils import parse_yaml_request, make_yaml_response

DB = "database_3.db"
app = Flask("Log_MS")

def init_db():
    create = not os.path.exists(DB)
    conn = sqlite3.connect(DB)
    if create:
        conn.execute('''CREATE TABLE logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            event TEXT,
            details TEXT
        )''')
        conn.commit()
    conn.close()

@app.route("/log", methods=["POST"])
def log_event():
    body = parse_yaml_request(request)
    src = body.get("source")
    event = body.get("event")
    details = yaml.safe_dump(body.get("details", {}))
    conn = sqlite3.connect(DB)
    conn.execute("INSERT INTO logs(source,event,details) VALUES(?,?,?)", (src,event,details))
    conn.commit()
    conn.close()
    return make_yaml_response({"status":"OK","message":"log stored"})

@app.route("/logs", methods=["GET"])
def get_logs():
    conn = sqlite3.connect(DB)
    cur = conn.execute("SELECT id, ts, source, event, details FROM logs ORDER BY id DESC LIMIT 100")
    rows = [{"id":r[0],"ts":r[1],"source":r[2],"event":r[3],"details":yaml.safe_load(r[4]) if r[4] else {}} for r in cur.fetchall()]
    conn.close()
    return make_yaml_response({"status":"OK","logs":rows})

if __name__ == "__main__":
    init_db()
    app.run(port=5007, host="0.0.0.0")

#(Sender_MS — external initiating microservice)
from flask import Flask, request
import requests, yaml
from utils import parse_yaml_request, make_yaml_response

app = Flask("Sender_MS")
UI_URL = "http://localhost:5002/request_delivery"

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    body = parse_yaml_request(request)
    # send to UI_MS
    r = requests.post(UI_URL, data=yaml.safe_dump(body), headers={"Content-Type":"application/x-yaml"})
    return make_yaml_response(yaml.safe_load(r.text))

@app.route("/notify", methods=["POST"])
def notify():
    body = parse_yaml_request(request)
    # Sender acknowledges UI_MS
    return make_yaml_response({"status":"OK","message":"sender received notification","details":body})

@app.route("/notify_update", methods=["POST"])
def notify_update():
    body = parse_yaml_request(request)
    return make_yaml_response({"status":"OK","message":"sender received update","details":body})

if __name__ == "__main__":
    app.run(port=5001, host="0.0.0.0")

#(Storage_MS — manages Database_1 and Database_2)
from flask import Flask, request
import sqlite3, os, yaml
from utils import parse_yaml_request, make_yaml_response

DB1 = "database_1.db"  # parcel/delivery storage
DB2 = "database_2.db"  # assignment storage

app = Flask("Storage_MS")

def init_db():
    for db, create_stmt in [
        (DB1, '''CREATE TABLE IF NOT EXISTS deliveries (
                    id TEXT PRIMARY KEY,
                    parcel TEXT,
                    car_id TEXT,
                    status TEXT,
                    metadata TEXT
                 )'''),
        (DB2, '''CREATE TABLE IF NOT EXISTS assignments (
                    key TEXT PRIMARY KEY,
                    value TEXT
                 )''')
    ]:
        conn = sqlite3.connect(db)
        conn.execute(create_stmt)
        conn.commit()
        conn.close()

@app.route("/store_id", methods=["POST"])
def store_id():
    body = parse_yaml_request(request)
    parcel_id = body.get("parcel_id")
    # store in DB2 (assignment store) as an entry
    conn = sqlite3.connect(DB2)
    conn.execute("INSERT OR REPLACE INTO assignments(key,value) VALUES(?,?)", (f"parcel:{parcel_id}", yaml.safe_dump(body)))
    conn.commit(); conn.close()
    return make_yaml_response({"status":"OK","message":"parcel_id stored"})

@app.route("/store_car", methods=["POST"])
def store_car():
    body = parse_yaml_request(request)
    car_id = body.get("car_id")
    conn = sqlite3.connect(DB2)
    conn.execute("INSERT OR REPLACE INTO assignments(key,value) VALUES(?,?)", (f"car:{car_id}", yaml.safe_dump(body)))
    conn.commit(); conn.close()
    return make_yaml_response({"status":"OK","message":"car_id stored"})

@app.route("/get_parcel/<parcel_id>", methods=["GET"])
def get_parcel(parcel_id):
    conn = sqlite3.connect(DB2)
    cur = conn.execute("SELECT value FROM assignments WHERE key = ?", (f"parcel:{parcel_id}",))
    row = cur.fetchone()
    conn.close()
    if not row: return make_yaml_response({"status":"NOT_FOUND"}, status=404)
    return Response(row[0], mimetype="application/x-yaml")

@app.route("/get_car/<car_id>", methods=["GET"])
def get_car(car_id):
    conn = sqlite3.connect(DB2)
    cur = conn.execute("SELECT value FROM assignments WHERE key = ?", (f"car:{car_id}",))
    row = cur.fetchone()
    conn.close()
    if not row: return make_yaml_response({"status":"NOT_FOUND"}, status=404)
    return Response(row[0], mimetype="application/x-yaml")

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    body = parse_yaml_request(request)
    parcel_id = body.get("parcel_id")
    car_id = body.get("car_id")
    delivery = body.get("delivery", {})
    conn = sqlite3.connect(DB1)
    conn.execute("INSERT OR REPLACE INTO deliveries(id, parcel, car_id, status, metadata) VALUES(?,?,?,?,?)",
                 (parcel_id, yaml.safe_dump(delivery.get("parcel_data", {})), car_id, delivery.get("status","assigned"), yaml.safe_dump(delivery.get("meta",{}))))
    conn.commit(); conn.close()
    # also store mapping in DB2 for quick lookup
    conn2 = sqlite3.connect(DB2)
    conn2.execute("INSERT OR REPLACE INTO assignments(key,value) VALUES(?,?)", (f"delivery:{parcel_id}", yaml.safe_dump(body)))
    conn2.commit(); conn2.close()
    return make_yaml_response({"status":"OK","message":"delivery stored"})

@app.route("/update_delivery", methods=["POST"])
def update_delivery():
    body = parse_yaml_request(request)
    parcel_id = body.get("parcel_id")
    updates = body.get("updates", {})
    conn = sqlite3.connect(DB1)
    cur = conn.execute("SELECT metadata FROM deliveries WHERE id = ?", (parcel_id,))
    row = cur.fetchone()
    if not row:
        # not found
        conn.close()
        return make_yaml_response({"status":"NOT_FOUND"}, status=404)
    # just overwrite status/metadata for simplicity
    status = updates.get("status")
    meta = updates.get("meta", {})
    if status:
        conn.execute("UPDATE deliveries SET status = ?, metadata = ? WHERE id = ?", (status, yaml.safe_dump(meta), parcel_id))
    else:
        conn.execute("UPDATE deliveries SET metadata = ? WHERE id = ?", (yaml.safe_dump(meta), parcel_id))
    conn.commit(); conn.close()
    return make_yaml_response({"status":"OK","message":"delivery updated"})

if __name__ == "__main__":
    init_db()
    app.run(port=5005, host="0.0.0.0")

#(UI_MS — forwards sender requests and notifies sender)
from flask import Flask, request
import requests, yaml
from utils import parse_yaml_request, make_yaml_response

app = Flask("UI_MS")
CONTROLLER_URL = "http://localhost:5004/request_delivery"

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    body = parse_yaml_request(request)
    # forward to Controller_MS
    r = requests.post(CONTROLLER_URL, data=yaml.safe_dump(body), headers={"Content-Type":"application/x-yaml"})
    return make_yaml_response(yaml.safe_load(r.text))

@app.route("/notify_delivery", methods=["POST"])
def notify_delivery():
    body = parse_yaml_request(request)
    # UI notifies Sender_MS (assume Sender is reachable)
    try:
        requests.post("http://localhost:5001/notify", data=yaml.safe_dump(body), headers={"Content-Type":"application/x-yaml"})
    except Exception:
        pass
    # Acknowledge Controller_MS
    return make_yaml_response({"status":"OK","message":"UI notified sender"})

@app.route("/notify_update", methods=["POST"])
def notify_update():
    body = parse_yaml_request(request)
    # Notify Sender about updates
    try:
        requests.post("http://localhost:5001/notify_update", data=yaml.safe_dump(body), headers={"Content-Type":"application/x-yaml"})
    except Exception:
        pass
    return make_yaml_response({"status":"OK","message":"update notified to sender"})

if __name__ == "__main__":
    app.run(port=5002, host="0.0.0.0")

#(shared helper; copy into each service folder or the same folder)
import yaml
from flask import Request, Response

YAML_CT = "application/x-yaml"

def parse_yaml_request(request: 'Request'):
    raw = request.data.decode('utf-8') if request.data else ''
    if not raw.strip():
        return {}
    return yaml.safe_load(raw)

def make_yaml_response(obj, status=200):
    body = yaml.safe_dump(obj)
    return Response(body, status=status, mimetype=YAML_CT)

