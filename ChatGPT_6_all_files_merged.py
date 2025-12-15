# car_ms.py
from flask import Flask
from utils import yaml_response, parse_yaml_request
import time
import uuid

app = Flask(__name__)
CAR_ID = f"CAR-{uuid.uuid4().hex[:8].upper()}"

@app.route("/check", methods=["GET"])
def check():
    # returns car id
    return yaml_response({"car_id": CAR_ID})

@app.route("/notify", methods=["POST"])
def notify():
    payload = parse_yaml_request()
    # Controller notifies the car of assigned delivery
    # Car acknowledges back
    ack = {"status":"acknowledged","car_id":CAR_ID,"received": payload}
    return yaml_response(ack)

@app.route("/request_update", methods=["POST"])
def request_update():
    # Car asks controller for updates. Car will send its request, Controller responds.
    payload = parse_yaml_request()
    # in our simulation the car simply forwards to Controller in real life; for now we return a dummy
    return yaml_response({"status":"waiting_for_update"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005)

# controller_ms.py
from flask import Flask
import requests
import yaml
from utils import yaml_response, parse_yaml_request, YAML_MIMETYPE
import time

app = Flask(__name__)

IDGEN_URL = "http://localhost:5003"
STORAGE_URL = "http://localhost:5004"
CAR_URL = "http://localhost:5005"
LOG_URL = "http://localhost:5006"
UI_URL = "http://localhost:5001"

def send_log(source, level, message):
    payload = {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
               "source": source,
               "level": level,
               "message": message}
    headers = {"Content-Type": YAML_MIMETYPE}
    try:
        requests.post(f"{LOG_URL}/log", data=yaml.safe_dump(payload), headers=headers, timeout=5)
    except:
        pass

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    # Called by UI_MS. Orchestrate the flow described.
    payload = parse_yaml_request()
    # 1) Request parcel id from IDGen_MS
    send_log("Controller_MS","INFO","Requesting parcel ID from IDGen_MS")
    headers = {"Content-Type": YAML_MIMETYPE}
    r = requests.post(f"{IDGEN_URL}/generate", data=yaml.safe_dump({"request": "parcel_id"}), headers=headers, timeout=5)
    idgen_resp = yaml.safe_load(r.text)
    parcel_id = idgen_resp.get("parcel_id")
    send_log("Controller_MS","INFO",f"Received parcel_id {parcel_id} from IDGen_MS")

    # 2) Request car id from Car_MS
    send_log("Controller_MS","INFO","Requesting car ID from Car_MS")
    r2 = requests.get(f"{CAR_URL}/check", timeout=5)
    car_resp = yaml.safe_load(r2.text)
    car_id = car_resp.get("car_id")
    send_log("Controller_MS","INFO",f"Received car_id {car_id} from Car_MS")

    # 3) Share car id with Storage_MS (Storage stores it in DB2)
    send_log("Controller_MS","INFO","Storing car id in Storage_MS")
    store_car = {"type":"car","id":car_id,"stored_at":time.strftime("%Y-%m-%d %H:%M:%S")}
    requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(store_car), headers=headers, timeout=5)

    # 4) Ask Storage_MS for parcel & car (in case)
    send_log("Controller_MS","INFO","Requesting latest IDs from Storage_MS")
    r3 = requests.get(f"{STORAGE_URL}/get_ids", timeout=5)
    ids = yaml.safe_load(r3.text)
    # fallback to previously obtained ids
    if not ids.get("parcel_id"):
        ids["parcel_id"] = parcel_id
    if not ids.get("car_id"):
        ids["car_id"] = car_id

    # 5) Assign delivery
    send_log("Controller_MS","INFO",f"Assigning delivery parcel={ids['parcel_id']} car={ids['car_id']}")
    delivery_payload = {"parcel_id": ids["parcel_id"], "car_id": ids["car_id"], "status":"assigned", "created_at":time.strftime("%Y-%m-%d %H:%M:%S")}
    requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(delivery_payload), headers=headers, timeout=5)

    # 6) Notify Car_MS
    notify_payload = {"parcel_id": ids["parcel_id"], "car_id": ids["car_id"], "action":"delivery_assigned"}
    r4 = requests.post(f"{CAR_URL}/notify", data=yaml.safe_dump(notify_payload), headers=headers, timeout=5)
    car_ack = yaml.safe_load(r4.text)

    # 7) Notify UI_MS (so it can notify Sender_MS)
    requests.post(f"{UI_URL}/notify_assignment", data=yaml.safe_dump({"parcel_id": ids["parcel_id"], "car_id": ids["car_id"]}), headers=headers, timeout=5)

    send_log("Controller_MS","INFO","Delivery assigned and parties notified")
    return yaml_response({"status":"ok","parcel_id": ids["parcel_id"], "car_id": ids["car_id"], "car_ack": car_ack})

@app.route("/car_update_request", methods=["POST"])
def car_update_request():
    # Called by Car_MS to request latest delivery update / or send updates
    payload = parse_yaml_request()
    # acknowledge the car
    send_log("Controller_MS","INFO","Received update request from Car_MS")
    # Instruct Storage to update any delivery if payload contains parcel_id/status
    if payload.get("parcel_id") and payload.get("status"):
        update_payload = {
            "parcel_id": payload["parcel_id"],
            "car_id": payload.get("car_id"),
            "status": payload["status"],
            "metadata": payload.get("metadata", ""),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        # For simplicity, store a new delivery entry with updated status
        headers = {"Content-Type": YAML_MIMETYPE}
        requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(update_payload), headers=headers, timeout=5)
        send_log("Controller_MS","INFO",f"Updated delivery {payload['parcel_id']} status {payload['status']} in Storage_MS")

    return yaml_response({"status":"acknowledged"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)

# idgen_ms.py
from flask import Flask
import uuid
import requests
import yaml
from utils import yaml_response, parse_yaml_request, YAML_MIMETYPE
import time

app = Flask(__name__)
STORAGE_URL = "http://localhost:5004"  # Storage_MS
CONTROLLER_ACK_ENDPOINT = "/id_ack"   # controller will have /id_ack

@app.route("/generate", methods=["POST"])
def generate():
    payload = parse_yaml_request()
    # Generate parcel id
    parcel_id = f"PID-{uuid.uuid4().hex[:12].upper()}"
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    # share with Storage_MS
    store_payload = {"type":"parcel", "id":parcel_id, "stored_at":ts}
    headers = {"Content-Type": YAML_MIMETYPE}
    try:
        r = requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(store_payload), headers=headers, timeout=5)
        stored = r.status_code == 200
    except Exception as e:
        stored = False

    # Acknowledge Controller_MS — in this simplified version we return the parcel id in response
    return yaml_response({"parcel_id":parcel_id, "storage_ack": stored})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003)

# log_ms.py
from flask import Flask
import sqlite3
import yaml
from utils import yaml_response, parse_yaml_request, YAML_MIMETYPE
import time

app = Flask(__name__)
DB = "db3.sqlite"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS logs (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp TEXT,
                 source TEXT,
                 level TEXT,
                 message TEXT
                 )""")
    conn.commit()
    conn.close()

@app.route("/log", methods=["POST"])
def log():
    payload = parse_yaml_request()
    ts = payload.get("timestamp") or time.strftime("%Y-%m-%d %H:%M:%S")
    source = payload.get("source", "unknown")
    level = payload.get("level", "INFO")
    message = payload.get("message", "")

    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute("INSERT INTO logs(timestamp, source, level, message) VALUES (?, ?, ?, ?)",
              (ts, source, level, message))
    conn.commit()
    conn.close()

    return yaml_response({"status":"ok","stored": True})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5006)

# sender_ms.py
from flask import Flask
from utils import yaml_response, parse_yaml_request, YAML_MIMETYPE
import requests
import yaml

app = Flask(__name__)
UI_URL = "http://localhost:5001"

@app.route("/initiate", methods=["POST"])
def initiate():
    payload = parse_yaml_request()
    headers = {"Content-Type": YAML_MIMETYPE}
    r = requests.post(f"{UI_URL}/request_delivery", data=yaml.safe_dump(payload), headers=headers, timeout=10)
    return yaml_response({"status":"initiated","ui_response": yaml.safe_load(r.text)})

@app.route("/notify", methods=["POST"])
def notify():
    payload = parse_yaml_request()
    # Sender acknowledges UI
    # For demonstration, just return ack
    return yaml_response({"status":"received_by_sender","payload": payload})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

# storage_ms.py
from flask import Flask
import sqlite3
from utils import yaml_response, parse_yaml_request
import time

app = Flask(__name__)
DB1 = "db1.sqlite"  # parcel/delivery data
DB2 = "db2.sqlite"  # assignments (parcel ID <-> car ID)

def init_db():
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS deliveries (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 parcel_id TEXT,
                 car_id TEXT,
                 status TEXT,
                 metadata TEXT,
                 created_at TEXT
                 )""")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS assignments (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 parcel_id TEXT,
                 car_id TEXT,
                 stored_at TEXT
                 )""")
    conn.commit()
    conn.close()

@app.route("/store_id", methods=["POST"])
def store_id():
    # used for storing parcel or car IDs in DB2
    payload = parse_yaml_request()
    typ = payload.get("type")  # "parcel" or "car"
    idval = payload.get("id")
    stored_at = payload.get("stored_at") or time.strftime("%Y-%m-%d %H:%M:%S")
    if not typ or not idval:
        return yaml_response({"error":"missing type or id"}, status=400)
    # store a row in assignments with either parcel_id or car_id (the other null)
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    if typ == "parcel":
        c.execute("INSERT INTO assignments(parcel_id, car_id, stored_at) VALUES (?, ?, ?)",
                  (idval, None, stored_at))
    elif typ == "car":
        c.execute("INSERT INTO assignments(parcel_id, car_id, stored_at) VALUES (?, ?, ?)",
                  (None, idval, stored_at))
    else:
        return yaml_response({"error":"invalid type"}, status=400)
    conn.commit()
    conn.close()
    return yaml_response({"status":"ok","type":typ,"id":idval})

@app.route("/get_ids", methods=["GET"])
def get_ids():
    # returns latest parcel and car ids available
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    c.execute("SELECT parcel_id FROM assignments WHERE parcel_id IS NOT NULL ORDER BY id DESC LIMIT 1")
    parcel_row = c.fetchone()
    c.execute("SELECT car_id FROM assignments WHERE car_id IS NOT NULL ORDER BY id DESC LIMIT 1")
    car_row = c.fetchone()
    conn.close()
    return yaml_response({"parcel_id": parcel_row[0] if parcel_row else None,
                          "car_id": car_row[0] if car_row else None})

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    # stores full assignment/delivery to DB1
    payload = parse_yaml_request()
    parcel_id = payload.get("parcel_id")
    car_id = payload.get("car_id")
    status = payload.get("status", "assigned")
    metadata = payload.get("metadata", "")
    created_at = payload.get("created_at") or time.strftime("%Y-%m-%d %H:%M:%S")
    if not parcel_id or not car_id:
        return yaml_response({"error":"missing parcel_id or car_id"}, status=400)
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    c.execute("INSERT INTO deliveries(parcel_id, car_id, status, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
              (parcel_id, car_id, status, metadata, created_at))
    conn.commit()
    conn.close()
    return yaml_response({"status":"ok","parcel_id":parcel_id,"car_id":car_id})

@app.route("/get_delivery/<parcel_id>", methods=["GET"])
def get_delivery(parcel_id):
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    c.execute("SELECT parcel_id, car_id, status, metadata, created_at FROM deliveries WHERE parcel_id = ?",
              (parcel_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return yaml_response({"error":"not_found"}, status=404)
    return yaml_response({"parcel_id":row[0],"car_id":row[1],"status":row[2],"metadata":row[3],"created_at":row[4]})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5004)

# ui_ms.py
from flask import Flask
import requests
import yaml
from utils import yaml_response, parse_yaml_request, YAML_MIMETYPE
import time

app = Flask(__name__)
CONTROLLER_URL = "http://localhost:5002"
SENDER_CALLBACK_PORT = 5000  # we assume Sender listens

def forward_log(source, message):
    pass  # controller handles logging centrally in this architecture

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    payload = parse_yaml_request()
    headers = {"Content-Type": YAML_MIMETYPE}
    # forward to controller
    r = requests.post(f"{CONTROLLER_URL}/request_delivery", data=yaml.safe_dump(payload), headers=headers, timeout=10)
    resp = yaml.safe_load(r.text)
    return yaml_response({"status":"forwarded","controller_response": resp})

@app.route("/notify_assignment", methods=["POST"])
def notify_assignment():
    # Controller notifies UI then UI notifies Sender
    payload = parse_yaml_request()
    # notify sender
    # in a real world, we'd have sender address; for demo assume local Sender_MS endpoint
    try:
        headers = {"Content-Type": YAML_MIMETYPE}
        requests.post("http://localhost:5000/notify", data=yaml.safe_dump(payload), headers=headers, timeout=5)
    except Exception as e:
        pass
    # also ack back to controller
    return yaml_response({"status":"notified_sender"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

# utils.py
import yaml
from flask import Response, request

YAML_MIMETYPE = "application/x-yaml"

def yaml_response(obj, status=200):
    text = yaml.safe_dump(obj)
    return Response(text, status=status, mimetype=YAML_MIMETYPE)

def parse_yaml_request():
    # returns dict parsed from YAML body
    if request.mimetype != YAML_MIMETYPE and not request.data:
        # try parse anyway
        text = request.get_data(as_text=True)
    else:
        text = request.get_data(as_text=True)
    if not text:
        return {}
    return yaml.safe_load(text)

