#Checks car ID and shares with Storage_MS. Also receives notifications and requests updates.
# car_ms.py
from flask import Flask, request, Response
import yaml, requests
from datetime import datetime

app = Flask("Car_MS")

STORAGE_URL = "http://localhost:8002/store_id"
CONTROLLER_URL = "http://localhost:8000/car_ack"
LOG_URL = "http://localhost:8006/log"

# For demo: pretend we have a list of valid car IDs
VALID_CARS = {"CAR-001": True, "CAR-002": True, "CAR-003": True}

@app.route("/check_car", methods=["POST"])
def check_car():
    payload = yaml.safe_load(request.data)
    car_id = payload.get("car_id")
    headers = {'Content-Type':'application/x-yaml'}
    # simulate check
    ok = VALID_CARS.get(car_id, True)  # default allow (for demo)
    # share car ID with Storage_MS
    store_msg = {"type": "car", "id": car_id, "timestamp": datetime.utcnow().isoformat()}
    try:
        r = requests.post(STORAGE_URL, data=yaml.safe_dump(store_msg), headers=headers, timeout=5)
        store_resp = yaml.safe_load(r.content)
    except Exception as e:
        store_resp = {"status":"error", "detail": str(e)}
    # ack back to controller
    ack = {"status":"ok" if ok else "error", "car_id": car_id, "stored": store_resp}
    # send ack to controller endpoint if provided
    # (controller usually called car_ms and awaits ack in flow; we return ack)
    try:
        log = {"timestamp": datetime.utcnow().isoformat(), "source": "Car_MS", "event":"checked_car", "details": ack}
        requests.post(LOG_URL, data=yaml.safe_dump(log), headers=headers, timeout=2)
    except:
        pass
    return Response(yaml.safe_dump(ack), mimetype="application/x-yaml")

@app.route("/notify", methods=["POST"])
def notify():
    payload = yaml.safe_load(request.data)
    # e.g. controller notifies car of assignment
    # return acknowledgment
    ack = {"status":"ok", "received": True, "info": payload}
    # log
    try:
        headers = {'Content-Type':'application/x-yaml'}
        log = {"timestamp": datetime.utcnow().isoformat(), "source": "Car_MS", "event": "notified", "details": payload}
        requests.post(LOG_URL, data=yaml.safe_dump(log), headers=headers, timeout=2)
    except:
        pass
    return Response(yaml.safe_dump(ack), mimetype="application/x-yaml")

@app.route("/request_update", methods=["POST"])
def request_update():
    # car asks controller for delivery update
    payload = yaml.safe_load(request.data) or {}
    controller_url = "http://localhost:8000/car_request_update"
    headers = {'Content-Type':'application/x-yaml'}
    try:
        r = requests.post(controller_url, data=yaml.safe_dump(payload), headers=headers, timeout=5)
        resp = yaml.safe_load(r.content)
    except Exception as e:
        resp = {"status":"error", "detail": str(e)}
    return Response(yaml.safe_dump(resp), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(port=8004, host="0.0.0.0")

#The central orchestrator implementing the flow, calls IDGen, Car, Storage, Log, and UI.
# controller_ms.py
from flask import Flask, request, Response
import yaml, requests
from datetime import datetime
import time

app = Flask("Controller_MS")

# CONFIG (change hostnames to match actual host deployment)
IDGEN_URL = "http://localhost:8003/request_id"
CAR_CHECK_URL = "http://localhost:8004/check_car"
STORAGE_GET_URL = "http://localhost:8002/get_ids"
STORAGE_STORE_DELIVERY = "http://localhost:8002/store_delivery"
LOG_URL = "http://localhost:8006/log"
UI_NOTIFY = "http://localhost:8001/notify"  # UI_MS notify endpoint
CAR_NOTIFY = "http://localhost:8004/notify"  # Car notify

HEADERS = {'Content-Type': 'application/x-yaml'}

def send_log(event, details):
    payload = {"timestamp": datetime.utcnow().isoformat(), "source": "Controller_MS", "event": event, "details": details}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=2)
    except:
        pass

@app.route("/request_delivery_from_ui", methods=["POST"])
def request_from_ui():
    payload = yaml.safe_load(request.data)
    # UI forwarded a request delivery -> start orchestration
    # 1. request parcel ID
    send_log("received_request_delivery", payload)
    try:
        id_resp = requests.post(IDGEN_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=5)
        id_resp_obj = yaml.safe_load(id_resp.content)
        parcel_id = id_resp_obj.get("parcel_id")
    except Exception as e:
        send_log("idgen_error", {"error": str(e)})
        return Response(yaml.safe_dump({"status":"error","detail":str(e)}), mimetype="application/x-yaml", status=500)

    send_log("got_parcel_id", {"parcel_id": parcel_id})

    # 2. request car ID from Car_MS (for demo, Car_MS will check a car_id provided or we choose one)
    # For demo assume payload may contain preferred_car or controller decides
    chosen_car = payload.get("preferred_car", "CAR-001")
    car_check_payload = {"car_id": chosen_car}
    try:
        car_check_resp = requests.post(CAR_CHECK_URL, data=yaml.safe_dump(car_check_payload), headers=HEADERS, timeout=5)
        car_check_obj = yaml.safe_load(car_check_resp.content)
    except Exception as e:
        send_log("car_check_error", {"error": str(e)})
        return Response(yaml.safe_dump({"status":"error","detail":str(e)}), mimetype="application/x-yaml", status=500)

    send_log("got_car_id", {"car_check": car_check_obj})

    # 3. ask Storage for the saved IDs (demo; Storage already got IDs from IDGen & Car)
    try:
        s_resp = requests.post(STORAGE_GET_URL, data=yaml.safe_dump({}), headers=HEADERS, timeout=5)
        s_obj = yaml.safe_load(s_resp.content)
    except Exception as e:
        s_obj = {"error": str(e)}
    send_log("storage_ids", s_obj)

    # 4. assign delivery: create a delivery record and store in Database_1 via storage_ms
    delivery = {"parcel_id": parcel_id, "car_id": chosen_car, "status": "assigned", "details": payload}
    try:
        store_delivery_resp = requests.post(STORAGE_STORE_DELIVERY, data=yaml.safe_dump(delivery), headers=HEADERS, timeout=5)
        store_delivery_obj = yaml.safe_load(store_delivery_resp.content)
    except Exception as e:
        store_delivery_obj = {"status":"error","detail": str(e)}
    send_log("delivery_stored", store_delivery_obj)

    # 5. notify car
    try:
        notify_resp = requests.post(CAR_NOTIFY, data=yaml.safe_dump({"parcel_id": parcel_id, "car_id": chosen_car}), headers=HEADERS, timeout=5)
        notify_obj = yaml.safe_load(notify_resp.content)
    except Exception as e:
        notify_obj = {"status":"error","detail": str(e)}
    send_log("car_notified", notify_obj)

    # 6. notify UI and ultimately Sender (UI will forward to Sender)
    try:
        ui_resp = requests.post(UI_NOTIFY, data=yaml.safe_dump({"parcel_id": parcel_id, "car_id": chosen_car, "status":"assigned"}), headers=HEADERS, timeout=5)
        ui_obj = yaml.safe_load(ui_resp.content)
    except Exception as e:
        ui_obj = {"status":"error", "detail": str(e)}
    send_log("ui_notified", ui_obj)

    result = {"status":"ok", "parcel_id": parcel_id, "car_id": chosen_car, "storage": s_obj, "delivery_store": store_delivery_obj, "car_notify": notify_obj, "ui_notify": ui_obj}
    return Response(yaml.safe_dump(result), mimetype="application/x-yaml")

@app.route("/car_request_update", methods=["POST"])
def car_request_update():
    payload = yaml.safe_load(request.data) or {}
    # Car requests update -> controller acknowledges and pushes update to storage and UI
    send_log("car_requested_update", payload)
    # ack to car
    ack = {"status":"ok", "received": True, "timestamp": datetime.utcnow().isoformat()}
    # share delivery update with storage
    try:
        r = requests.post("http://localhost:8002/update_delivery", data=yaml.safe_dump(payload), headers=HEADERS, timeout=5)
        storage_resp = yaml.safe_load(r.content)
    except Exception as e:
        storage_resp = {"status":"error", "detail": str(e)}
    send_log("delivery_updated_in_storage", storage_resp)
    # notify UI
    try:
        ui_notify = requests.post("http://localhost:8001/notify", data=yaml.safe_dump(payload), headers=HEADERS, timeout=5)
    except:
        pass
    send_log("sent_update_to_ui", payload)
    return Response(yaml.safe_dump({"ack": ack, "storage_resp": storage_resp}), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(port=8000, host="0.0.0.0")

#Generates parcel IDs and informs Storage_MS; acknowledges Controller_MS.
# idgen_ms.py
from flask import Flask, request, Response
import yaml, requests
import uuid
from datetime import datetime

app = Flask("IDGen_MS")

# CONFIG - change hostnames when deploying across machines
STORAGE_URL = "http://localhost:8002/store_id"
CONTROLLER_CALLBACK = "http://localhost:8000/idgen_ack"  # controller endpoint to ack
LOG_URL = "http://localhost:8006/log"

@app.route("/request_id", methods=["POST"])
def request_id():
    payload = yaml.safe_load(request.data) or {}
    # Generate unique parcel ID
    parcel_id = f"PARCEL-{uuid.uuid4().hex[:12].upper()}"
    # share with Storage_MS
    store_msg = {"type": "parcel", "id": parcel_id, "timestamp": datetime.utcnow().isoformat()}
    headers = {'Content-Type': 'application/x-yaml'}
    try:
        r = requests.post(STORAGE_URL, data=yaml.safe_dump(store_msg), headers=headers, timeout=5)
        store_resp = yaml.safe_load(r.content)
    except Exception as e:
        store_resp = {"status":"error", "detail": str(e)}
    # acknowledge Controller_MS
    ack = {"status":"ok", "parcel_id": parcel_id, "stored": store_resp}
    # optionally notify log
    try:
        log = {"timestamp": datetime.utcnow().isoformat(), "source": "IDGen_MS", "event":"generated_parcel_id", "details": ack}
        requests.post(LOG_URL, data=yaml.safe_dump(log), headers=headers, timeout=2)
    except:
        pass
    return Response(yaml.safe_dump(ack), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(port=8003, host="0.0.0.0")

Stores logs to db_database3.sqlite.
# log_ms.py
from flask import Flask, request, Response
import yaml
import sqlite3
import time
from datetime import datetime

DB = "db_database3.sqlite"
app = Flask("Log_MS")

def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        source TEXT,
        event TEXT,
        details TEXT
    )
    """)
    conn.commit()
    conn.close()

@app.route("/log", methods=["POST"])
def receive_log():
    payload = yaml.safe_load(request.data)
    timestamp = payload.get("timestamp", datetime.utcnow().isoformat())
    source = payload.get("source", "unknown")
    event = payload.get("event", "")
    details = yaml.safe_dump(payload.get("details", {}))
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("INSERT INTO logs (timestamp, source, event, details) VALUES (?, ?, ?, ?)",
                (timestamp, source, event, details))
    conn.commit()
    conn.close()
    response = {"status": "ok", "stored": True, "received_at": datetime.utcnow().isoformat()}
    return Response(yaml.safe_dump(response), mimetype="application/x-yaml")

if __name__ == "__main__":
    init_db()
    app.run(port=8006, host="0.0.0.0")

#External service that requests delivery through UI_MS and handles acknowledgements.
# sender_ms.py
from flask import Flask, request, Response
import yaml, requests
from datetime import datetime

app = Flask("Sender_MS")

UI_URL = "http://localhost:8001/request_delivery"
HEADERS = {'Content-Type':'application/x-yaml'}
LOG_URL = "http://localhost:8006/log"

@app.route("/send_request", methods=["POST"])
def send_request():
    # the sender sends a delivery request to UI_MS; for demo we will call UI_MS from here
    payload = yaml.safe_load(request.data) or {}
    payload["sent_at"] = datetime.utcnow().isoformat()
    # call UI_MS
    try:
        r = requests.post(UI_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=10)
        resp = yaml.safe_load(r.content)
    except Exception as e:
        resp = {"status": "error", "detail": str(e)}
    # log
    try:
        log = {"timestamp": datetime.utcnow().isoformat(), "source": "Sender_MS", "event": "sent_request", "details": {"payload": payload, "response": resp}}
        requests.post(LOG_URL, data=yaml.safe_dump(log), headers=HEADERS, timeout=2)
    except:
        pass
    return Response(yaml.safe_dump(resp), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(port=8005, host="0.0.0.0")

#Stores parcel IDs & car IDs in Database_2 and deliveries in Database_1.
# storage_ms.py
from flask import Flask, request, Response
import yaml, sqlite3
from datetime import datetime

DB1 = "db_database1.sqlite"  # parcel/delivery
DB2 = "db_database2.sqlite"  # assignment (IDs)
app = Flask("Storage_MS")

def init_db():
    conn = sqlite3.connect(DB1)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        parcel_id TEXT,
        car_id TEXT,
        status TEXT,
        created_at TEXT,
        updated_at TEXT,
        details TEXT
    )
    """)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(DB2)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT,
        value TEXT,
        created_at TEXT
    )
    """)
    conn.commit()
    conn.close()

@app.route("/store_id", methods=["POST"])
def store_id():
    payload = yaml.safe_load(request.data)
    # payload expected: {type: "parcel"|"car", id: "<id>"}
    t = payload.get("type")
    _id = payload.get("id")
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB2)
    cur = conn.cursor()
    cur.execute("INSERT INTO assignments (key, value, created_at) VALUES (?, ?, ?)", (t, _id, now))
    conn.commit()
    conn.close()
    res = {"status":"ok", "action":"stored_id", "type": t, "id": _id}
    return Response(yaml.safe_dump(res), mimetype="application/x-yaml")

@app.route("/get_ids", methods=["POST"])
def get_ids():
    # Accept YAML filter: {type: "parcel"} or {type: "car"} or empty => return all
    payload = yaml.safe_load(request.data) or {}
    t = payload.get("type")
    conn = sqlite3.connect(DB2)
    cur = conn.cursor()
    if t:
        cur.execute("SELECT value FROM assignments WHERE key=?", (t,))
    else:
        cur.execute("SELECT key,value FROM assignments")
    rows = cur.fetchall()
    conn.close()
    if t:
        ids = [r[0] for r in rows]
        res = {"type": t, "ids": ids}
    else:
        res = {"assignments": [{"key": r[0], "value": r[1]} for r in rows]}
    return Response(yaml.safe_dump(res), mimetype="application/x-yaml")

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    payload = yaml.safe_load(request.data)
    parcel_id = payload.get("parcel_id")
    car_id = payload.get("car_id")
    status = payload.get("status", "assigned")
    details = payload.get("details", {})
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB1)
    cur = conn.cursor()
    cur.execute("""INSERT INTO deliveries (parcel_id, car_id, status, created_at, updated_at, details)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (parcel_id, car_id, status, now, now, yaml.safe_dump(details)))
    conn.commit()
    conn.close()
    res = {"status": "ok", "action": "store_delivery", "parcel_id": parcel_id, "car_id": car_id}
    return Response(yaml.safe_dump(res), mimetype="application/x-yaml")

@app.route("/update_delivery", methods=["POST"])
def update_delivery():
    payload = yaml.safe_load(request.data)
    parcel_id = payload.get("parcel_id")
    updates = payload.get("updates", {})
    now = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB1)
    cur = conn.cursor()
    # naive update: update status if provided
    if "status" in updates:
        cur.execute("UPDATE deliveries SET status=?, updated_at=? WHERE parcel_id=?",
                    (updates["status"], now, parcel_id))
    if "car_id" in updates:
        cur.execute("UPDATE deliveries SET car_id=?, updated_at=? WHERE parcel_id=?",
                    (updates["car_id"], now, parcel_id))
    conn.commit()
    conn.close()
    res = {"status":"ok", "action":"update_delivery", "parcel_id": parcel_id, "updates": updates}
    return Response(yaml.safe_dump(res), mimetype="application/x-yaml")

if __name__ == "__main__":
    init_db()
    app.run(port=8002, host="0.0.0.0")

#Receives request from Sender, forwards to Controller, notifies Sender of results.
# ui_ms.py
from flask import Flask, request, Response
import yaml, requests
from datetime import datetime

app = Flask("UI_MS")

CONTROLLER_URL = "http://localhost:8000/request_delivery_from_ui"
LOG_URL = "http://localhost:8006/log"
SENDER_CALLBACK = None  # For demo: sender will POST to this UI endpoint and await callbacks over HTTP response

HEADERS = {'Content-Type':'application/x-yaml'}

def send_log(event, details):
    payload = {"timestamp": datetime.utcnow().isoformat(), "source": "UI_MS", "event": event, "details": details}
    try:
        requests.post(LOG_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=2)
    except:
        pass

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    # Sender_MS sends here
    payload = yaml.safe_load(request.data) or {}
    send_log("received_request_from_sender", payload)
    try:
        r = requests.post(CONTROLLER_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=10)
        resp = yaml.safe_load(r.content)
    except Exception as e:
        resp = {"status":"error", "detail": str(e)}
    # Notify sender (we simply return resp to the sender's POST)
    send_log("forwarded_to_controller", resp)
    return Response(yaml.safe_dump(resp), mimetype="application/x-yaml")

@app.route("/notify", methods=["POST"])
def notify():
    # Controller notifies UI_MS of updates; UI then notifies sender (in this simple flow we return ack)
    payload = yaml.safe_load(request.data) or {}
    send_log("received_notification_from_controller", payload)
    # For demo we just acknowledge and pretend to notify Sender_MS
    ack = {"status":"ok", "notified_sender": True, "payload": payload}
    send_log("notified_sender", ack)
    return Response(yaml.safe_dump(ack), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(port=8001, host="0.0.0.0")

