#Car checks ID and responds. It also can request updates from Controller.
# car_ms.py
from flask import Flask
import os, yaml, requests
from common import yaml_request_data, yaml_response, now_iso

app = Flask(__name__)

STORAGE_MS_URL = os.environ.get("STORAGE_MS_URL", "http://localhost:6010")
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://localhost:6000/request_update")
LOG_MS_URL = os.environ.get("LOG_MS_URL", "http://localhost:6006/log")

# Example car registry (in-memory for sample)
CARS = {"CAR-100": {"status":"available"}, "CAR-200": {"status":"busy"}}

@app.route("/check_car", methods=["POST"])
def check_car():
    data = yaml_request_data()
    requested_car = data.get("car_id")
    if not requested_car:
        return yaml_response({"status":"error","reason":"no car_id provided"}, 400)
    # Check existence
    if requested_car in CARS:
        # share with Storage_MS
        try:
            headers = {"Content-Type":"application/x-yaml"}
            payload = {"car_id": requested_car, "ts": now_iso()}
            r = requests.post(f"{STORAGE_MS_URL}/store_car", data=yaml.safe_dump(payload), headers=headers, timeout=5)
            store_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
        except Exception as e:
            store_ack = {"status":"error","error":str(e)}
        # ack to caller
        try:
            requests.post(LOG_MS_URL, data=yaml.safe_dump({"origin":"Car_MS","level":"INFO","message":f"Car {requested_car} checked and stored","ts":now_iso()}), headers={"Content-Type":"application/x-yaml"})
        except:
            pass
        return yaml_response({"status":"ok","car_id":requested_car,"store_ack":store_ack})
    else:
        return yaml_response({"status":"not_found","car_id":requested_car}, 404)

@app.route("/notify", methods=["POST"])
def notify():
    data = yaml_request_data()
    # The Controller notifies Car that delivery assigned
    # Acknowledge back to Controller
    return yaml_response({"status":"ack","received":data})

@app.route("/request_update", methods=["POST"])
def request_update():
    data = yaml_request_data()
    # Car requests delivery update from Controller - forward to Controller endpoint
    try:
        headers = {"Content-Type":"application/x-yaml"}
        r = requests.post(os.environ.get("CONTROLLER_UPDATE_ENDPOINT", "http://localhost:6000/car_update_request"), data=yaml.safe_dump(data), headers=headers, timeout=5)
        ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
    except Exception as e:
        ack = {"status":"error","error":str(e)}
    return yaml_response({"status":"ok","controller_ack":ack})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("CAR_MS_PORT", 6020)))

#(helpers used by services: YAML I/O, sqlite helpers, small logger)
import yaml
from flask import Response, request
import sqlite3
import os
from datetime import datetime

YAML_MIME = "application/x-yaml"

def yaml_request_data():
    raw = request.data
    if not raw:
        return {}
    return yaml.safe_load(raw)

def yaml_response(obj, status=200):
    body = yaml.safe_dump(obj)
    return Response(body, status=status, mimetype=YAML_MIME)

def ensure_db(path, ddl_statements):
    first = not os.path.exists(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    if first:
        cur = conn.cursor()
        for ddl in ddl_statements:
            cur.execute(ddl)
        conn.commit()
    return conn

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

#The orchestrator that implements your full sequence. This is the longest piece — it drives the entire interaction chain.
# controller_ms.py
from flask import Flask
import os, requests, yaml, time
from common import yaml_request_data, yaml_response, now_iso

app = Flask(__name__)

IDGEN_URL = os.environ.get("IDGEN_URL", "http://localhost:6007/generate")
STORAGE_URL = os.environ.get("STORAGE_URL", "http://localhost:6010")
CAR_URL = os.environ.get("CAR_URL", "http://localhost:6020/check_car")
LOG_URL = os.environ.get("LOG_MS_URL", "http://localhost:6006/log")
UI_CALLBACK = os.environ.get("UI_CALLBACK", "http://localhost:6001/notify_from_controller")

HEADERS = {"Content-Type":"application/x-yaml"}

def log(origin, level, message):
    try:
        payload = {"origin": origin, "level": level, "message": message, "ts": now_iso()}
        requests.post(LOG_URL, data=yaml.safe_dump(payload), headers=HEADERS, timeout=3)
    except:
        pass

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    data = yaml_request_data()
    log("Controller_MS", "INFO", "Received request_delivery from UI_MS")
    # 1) request parcel ID from IDGen_MS
    try:
        r = requests.post(IDGEN_URL, data=yaml.safe_dump({"meta": data.get("meta")}), headers=HEADERS, timeout=5)
        idgen_resp = yaml.safe_load(r.text)
        parcel_id = idgen_resp.get("parcel_id")
        log("Controller_MS", "INFO", f"Got parcel id {parcel_id} from IDGen_MS")
    except Exception as e:
        return yaml_response({"status":"error","reason":"idgen_failed","error":str(e)}, 500)

    # 2) request car id from Car_MS (for example request specific or find available)
    requested_car = data.get("preferred_car", "CAR-100")
    try:
        r = requests.post(CAR_URL, data=yaml.safe_dump({"car_id": requested_car}), headers=HEADERS, timeout=5)
        car_resp = yaml.safe_load(r.text)
        if r.status_code != 200 or car_resp.get("status") != "ok":
            return yaml_response({"status":"error","reason":"car_check_failed","detail":car_resp}, 400)
        car_id = car_resp.get("car_id")
        log("Controller_MS", "INFO", f"Got car id {car_id} from Car_MS")
    except Exception as e:
        return yaml_response({"status":"error","reason":"car_request_failed","error":str(e)}, 500)

    # 3) read back parcel ID and car ID from Storage_MS (as per your flow)
    try:
        r1 = requests.get(f"{STORAGE_URL}/get_parcel/{parcel_id}", timeout=3)
        pinfo = yaml.safe_load(r1.text) if r1.ok else {}
    except:
        pinfo = {}

    try:
        r2 = requests.get(f"{STORAGE_URL}/get_car/{car_id}", timeout=3)
        cinfo = yaml.safe_load(r2.text) if r2.ok else {}
    except:
        cinfo = {}

    # 4) assign delivery and share with Storage_MS (store_delivery into DB1)
    delivery = {"parcel_id": parcel_id, "car_id": car_id, "status": "assigned", "ts": now_iso(), "meta": data.get("meta")}
    try:
        r = requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(delivery), headers=HEADERS, timeout=5)
        store_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
        log("Controller_MS", "INFO", f"Stored delivery {parcel_id} -> {car_id}")
    except Exception as e:
        store_ack = {"status":"error","error":str(e)}
        log("Controller_MS", "ERROR", f"Failed to store delivery: {e}")

    # 5) notify Car_MS
    try:
        r = requests.post(os.environ.get("CAR_NOTIFY_URL", "http://localhost:6020/notify"), data=yaml.safe_dump({"parcel_id": parcel_id, "car_id": car_id, "action":"assign"}), headers=HEADERS, timeout=5)
        car_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
    except Exception as e:
        car_ack = {"status":"error","error":str(e)}

    log("Controller_MS", "INFO", "Notified Car_MS about assignment")

    # 6) notify UI_MS (which will notify Sender_MS)
    try:
        r = requests.post(UI_CALLBACK, data=yaml.safe_dump({"parcel_id": parcel_id, "car_id": car_id, "status":"assigned"}), headers=HEADERS, timeout=5)
        ui_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
    except Exception as e:
        ui_ack = {"status":"error","error":str(e)}

    log("Controller_MS", "INFO", "Notified UI_MS about assignment")

    # final ack to caller (UI_MS)
    resp = {
        "status":"ok",
        "parcel_id": parcel_id,
        "car_id": car_id,
        "store_ack": store_ack,
        "car_ack": car_ack,
        "ui_ack": ui_ack
    }
    # final logging
    log("Controller_MS", "INFO", f"Completed assignment for {parcel_id} to {car_id}")
    return yaml_response(resp)

# Car requests delivery update from Controller_MS
@app.route("/car_update_request", methods=["POST"])
def car_update_request():
    data = yaml_request_data()
    car_id = data.get("car_id")
    parcel_id = data.get("parcel_id")
    # acknowledge car
    ack = {"status":"ack","received":data, "ts": now_iso()}
    # share delivery update with Storage_MS
    try:
        r = requests.post(f"{STORAGE_URL}/update_delivery", data=yaml.safe_dump({"parcel_id": parcel_id, "status": data.get("status", "in_transit"), "ts": now_iso()}), headers=HEADERS, timeout=5)
        storage_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
    except Exception as e:
        storage_ack = {"status":"error","error":str(e)}
    # notify UI_MS -> which notifies Sender_MS
    try:
        requests.post(UI_CALLBACK, data=yaml.safe_dump({"parcel_id": parcel_id, "car_id": car_id, "status": data.get("status", "in_transit")}), headers=HEADERS, timeout=5)
    except:
        pass
    # log
    log("Controller_MS", "INFO", f"Processed update for {parcel_id}: {data.get('status')}")
    return yaml_response({"ack":ack, "storage_ack": storage_ack})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("CONTROLLER_MS_PORT", 6000)))

#Generates parcel IDs and shares with Storage_MS.
from flask import Flask
import os, uuid, requests
from common import yaml_response, yaml_request_data, now_iso
import yaml

app = Flask(__name__)

STORAGE_MS_URL = os.environ.get("STORAGE_MS_URL", "http://localhost:6010")
LOG_MS_URL = os.environ.get("LOG_MS_URL", "http://localhost:6006/log")

@app.route("/generate", methods=["POST"])
def generate_id():
    req = yaml_request_data()
    # create a parcel id
    parcel_id = "P-" + uuid.uuid4().hex[:12].upper()
    payload = {"parcel_id": parcel_id, "ts": now_iso(), "meta": req.get("meta")}
    # share with Storage_MS
    try:
        headers = {"Content-Type": "application/x-yaml"}
        r = requests.post(f"{STORAGE_MS_URL}/store_id", data=yaml.safe_dump(payload), headers=headers, timeout=5)
        storage_ack = yaml.safe_load(r.text) if r.ok else {"status": "error"}
    except Exception as e:
        storage_ack = {"status": "error", "error": str(e)}
    # log to Log_MS (best-effort)
    try:
        requests.post(LOG_MS_URL, data=yaml.safe_dump({"origin":"IDGen_MS","level":"INFO","message":f"Generated parcel {parcel_id}","ts":now_iso()}), headers={"Content-Type":"application/x-yaml"})
    except:
        pass
    return yaml_response({"parcel_id": parcel_id, "storage_ack": storage_ack})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("IDGEN_MS_PORT", 6007)))

#Stores logs in Database_3 (logs.db).
from flask import Flask, request
import yaml, os
from common import yaml_request_data, yaml_response, ensure_db, now_iso

app = Flask(__name__)

DB_PATH = os.environ.get("LOG_DB", "logs.db")
DDL = [
    "CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, origin TEXT, level TEXT, message TEXT)"
]
conn = ensure_db(DB_PATH, DDL)

@app.route("/log", methods=["POST"])
def log_entry():
    data = yaml_request_data()
    origin = data.get("origin", "unknown")
    level = data.get("level", "INFO")
    message = data.get("message", "")
    ts = data.get("ts", now_iso())
    cur = conn.cursor()
    cur.execute("INSERT INTO logs (ts, origin, level, message) VALUES (?, ?, ?, ?)", (ts, origin, level, message))
    conn.commit()
    return yaml_response({"status": "ok", "stored_at": ts, "origin": origin})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("LOG_MS_PORT", 6006)))

#An external microservice on Laptop. It can send a delivery request to UI_MS and receive notifications.
# sender_ms.py
from flask import Flask
import os, yaml, requests
from common import yaml_request_data, yaml_response, now_iso

app = Flask(__name__)

UI_MS_URL = os.environ.get("UI_MS_URL", "http://localhost:6001/request_delivery")

@app.route("/notify", methods=["POST"])
def notify():
    # Notification from UI that controller completed or updated
    data = yaml_request_data()
    # Acknowledge to UI
    return yaml_response({"status":"ack","received":data})

@app.route("/ack", methods=["POST"])
def ack():
    data = yaml_request_data()
    # Received ack from UI that request was forwarded
    return yaml_response({"status":"ack","received":data})

if __name__ == "__main__":
    # Also allow sending a sample request when run as script
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "send":
        import requests, yaml
        payload = {"sender":"Sender_MS","pickup":"Location A","dropoff":"Location B","meta":{"weight":"2kg"}}
        headers = {"Content-Type":"application/x-yaml"}
        r = requests.post(UI_MS_URL, data=yaml.safe_dump(payload), headers=headers)
        print("UI response:", r.text)
        sys.exit(0)
    app.run(host="0.0.0.0", port=int(os.environ.get("SENDER_MS_PORT", 6030)))

#Handles Database_1 (deliveries) and Database_2 (assignments).
# storage_ms.py
from flask import Flask
import os, sqlite3, yaml
from common import yaml_request_data, yaml_response, ensure_db, now_iso

app = Flask(__name__)

DB_ASSIGN_PATH = os.environ.get("ASSIGN_DB", "assignments.db")  # Database_2
DB_DELIV_PATH = os.environ.get("DELIV_DB", "deliveries.db")    # Database_1

DDL_ASSIGN = [
    "CREATE TABLE IF NOT EXISTS assignments (id INTEGER PRIMARY KEY AUTOINCREMENT, parcel_id TEXT UNIQUE, car_id TEXT, ts TEXT)"
]
DDL_DELIV = [
    "CREATE TABLE IF NOT EXISTS deliveries (id INTEGER PRIMARY KEY AUTOINCREMENT, parcel_id TEXT, car_id TEXT, status TEXT, ts TEXT, meta TEXT)"
]

conn_assign = ensure_db(DB_ASSIGN_PATH, DDL_ASSIGN)
conn_deliv = ensure_db(DB_DELIV_PATH, DDL_DELIV)

@app.route("/store_id", methods=["POST"])
def store_id():
    data = yaml_request_data()
    parcel_id = data.get("parcel_id")
    ts = data.get("ts", now_iso())
    if not parcel_id:
        return yaml_response({"status":"error","reason":"no parcel_id"}, 400)
    cur = conn_assign.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO assignments (parcel_id, car_id, ts) VALUES (?, ?, ?)", (parcel_id, None, ts))
        conn_assign.commit()
        return yaml_response({"status":"ok","parcel_id":parcel_id})
    except Exception as e:
        return yaml_response({"status":"error","error":str(e)}, 500)

@app.route("/store_car", methods=["POST"])
def store_car():
    data = yaml_request_data()
    car_id = data.get("car_id")
    parcel_id = data.get("parcel_id")
    ts = data.get("ts", now_iso())
    if not car_id:
        return yaml_response({"status":"error","reason":"no car_id"}, 400)
    cur = conn_assign.cursor()
    try:
        # If parcel_id provided, set car for that parcel. Otherwise create separate record
        if parcel_id:
            cur.execute("UPDATE assignments SET car_id=? WHERE parcel_id=?", (car_id, parcel_id))
        else:
            cur.execute("INSERT INTO assignments (parcel_id, car_id, ts) VALUES (?, ?, ?)", (None, car_id, ts))
        conn_assign.commit()
        return yaml_response({"status":"ok","car_id":car_id, "parcel_id":parcel_id})
    except Exception as e:
        return yaml_response({"status":"error","error":str(e)}, 500)

@app.route("/get_parcel/<parcel_id>", methods=["GET"])
def get_parcel(parcel_id):
    cur = conn_assign.cursor()
    cur.execute("SELECT parcel_id, car_id, ts FROM assignments WHERE parcel_id=?", (parcel_id,))
    row = cur.fetchone()
    if not row:
        return yaml_response({"status":"not_found","parcel_id":parcel_id}, 404)
    return yaml_response({"status":"ok","parcel_id":row[0],"car_id":row[1],"ts":row[2]})

@app.route("/get_car/<car_id>", methods=["GET"])
def get_car(car_id):
    cur = conn_assign.cursor()
    cur.execute("SELECT parcel_id, car_id, ts FROM assignments WHERE car_id=?", (car_id,))
    row = cur.fetchone()
    if not row:
        return yaml_response({"status":"not_found","car_id":car_id}, 404)
    return yaml_response({"status":"ok","parcel_id":row[0],"car_id":row[1],"ts":row[2]})

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    data = yaml_request_data()
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    status = data.get("status", "assigned")
    meta = yaml.safe_dump(data.get("meta", {}))
    ts = data.get("ts", now_iso())
    if not parcel_id:
        return yaml_response({"status":"error","reason":"no parcel_id"}, 400)
    cur = conn_deliv.cursor()
    try:
        cur.execute("INSERT INTO deliveries (parcel_id, car_id, status, ts, meta) VALUES (?, ?, ?, ?, ?)", (parcel_id, car_id, status, ts, meta))
        conn_deliv.commit()
        return yaml_response({"status":"ok","parcel_id":parcel_id})
    except Exception as e:
        return yaml_response({"status":"error","error":str(e)}, 500)

@app.route("/update_delivery", methods=["POST"])
def update_delivery():
    data = yaml_request_data()
    parcel_id = data.get("parcel_id")
    status = data.get("status")
    ts = data.get("ts", now_iso())
    if not parcel_id or status is None:
        return yaml_response({"status":"error","reason":"parcel_id and status required"}, 400)
    cur = conn_deliv.cursor()
    cur.execute("UPDATE deliveries SET status=?, ts=? WHERE parcel_id=?", (status, ts, parcel_id))
    conn_deliv.commit()
    return yaml_response({"status":"ok","parcel_id":parcel_id, "new_status":status})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("STORAGE_MS_PORT", 6010)))

#Receives request from Sender_MS and forwards to Controller_MS; notifies Sender_MS on updates.
# ui_ms.py
from flask import Flask
import os, yaml, requests
from common import yaml_request_data, yaml_response, now_iso

app = Flask(__name__)

CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://localhost:6000/request_delivery")
SENDER_CALLBACK = os.environ.get("SENDER_CALLBACK", "http://localhost:6030/ack")
LOG_MS_URL = os.environ.get("LOG_MS_URL", "http://localhost:6006/log")

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    data = yaml_request_data()
    # Forward to Controller_MS
    try:
        headers = {"Content-Type":"application/x-yaml"}
        r = requests.post(CONTROLLER_URL, data=yaml.safe_dump(data), headers=headers, timeout=10)
        controller_resp = yaml.safe_load(r.text) if r.ok else {"status":"error"}
        # Acknowledge Sender
        try:
            requests.post(SENDER_CALLBACK, data=yaml.safe_dump({"status":"forwarded","controller":controller_resp}), headers=headers, timeout=3)
        except:
            pass
    except Exception as e:
        controller_resp = {"status":"error","error":str(e)}
    # Acknowledge UI -> Controller
    try:
        requests.post(LOG_MS_URL, data=yaml.safe_dump({"origin":"UI_MS","level":"INFO","message":"Forwarded delivery request to controller","ts":now_iso()}), headers={"Content-Type":"application/x-yaml"})
    except:
        pass
    return yaml_response({"status":"ok","controller_response":controller_resp})

@app.route("/notify_from_controller", methods=["POST"])
def notify_from_controller():
    data = yaml_request_data()
    # Controller notifies UI to inform Sender
    # Notify Sender_MS
    headers = {"Content-Type":"application/x-yaml"}
    sender_url = os.environ.get("SENDER_URL", "http://localhost:6030/notify")
    try:
        r = requests.post(sender_url, data=yaml.safe_dump(data), headers=headers, timeout=5)
        sender_ack = yaml.safe_load(r.text) if r.ok else {"status":"error"}
    except Exception as e:
        sender_ack = {"status":"error","error":str(e)}
    return yaml_response({"status":"ok","sender_ack":sender_ack})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("UI_MS_PORT", 6001)))

