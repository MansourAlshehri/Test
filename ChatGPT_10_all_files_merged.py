# car_ms.py
from flask import Flask, request, Response
import yaml, requests, time

app = Flask("Car_MS")
STORAGE_MS = "http://localhost:5005"
CONTROLLER_MS = "http://localhost:5003"
LOG_MS = "http://localhost:5007"

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

@app.route("/request_car", methods=["POST"])
def request_car():
    # Controller asked for a car
    data = yaml.safe_load(request.data) or {}
    # simple deterministic car id for demo
    car_id = f"CAR-{int(time.time())%10000}"
    # Share with Storage_MS
    try:
        requests.post(f"{STORAGE_MS}/store_car_id", data=yaml.safe_dump({"car_id":car_id}), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[Car_MS] failed storing car id:", e)
    # ack Controller
    try:
        requests.post(f"{LOG_MS}/log", data=yaml.safe_dump({"event":"car_issued","car_id":car_id,"ts":time.time()}), headers={"Content-Type":"application/x-yaml"})
    except:
        pass
    return yaml_response({"car_id":car_id, "ok":True})

@app.route("/notify_assignment", methods=["POST"])
def notify_assignment():
    data = yaml.safe_load(request.data) or {}
    print("[Car_MS] Received assignment:", data)
    # Acknowledge to Controller
    try:
        requests.post(f"{CONTROLLER_MS}/car_update_request", data=yaml.safe_dump({"parcel_id": data.get("parcel_id"), "car_id": data.get("car_id"), "status":"accepted"}), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[Car_MS] Could not inform Controller about acceptance:", e)
    return yaml_response({"status":"ack","from":"Car_MS"})

if __name__ == "__main__":
    app.run(port=5006, debug=True)

# controller_ms.py
from flask import Flask, request, Response
import yaml
import requests
import time

app = Flask("Controller_MS")
IDGEN_MS = "http://localhost:5004"
STORAGE_MS = "http://localhost:5005"
CAR_MS = "http://localhost:5006"
LOG_MS = "http://localhost:5007"
UI_MS = "http://localhost:5002"

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

def log_event(event):
    try:
        requests.post(f"{LOG_MS}/log", data=yaml.safe_dump(event), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[Controller_MS] Logging failed:", e)

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    data = yaml.safe_load(request.data) or {}
    print("[Controller_MS] Received request:", data)
    # 1. Request parcel ID from IDGen_MS
    r = requests.post(f"{IDGEN_MS}/generate_id", data=yaml.safe_dump({"purpose":"parcel"}), headers={"Content-Type":"application/x-yaml"})
    idgen_resp = yaml.safe_load(r.content)
    parcel_id = idgen_resp.get("parcel_id")
    # Log
    log_event({"event":"parcel_id_generated","parcel_id":parcel_id, "ts":time.time()})

    # 2. Request car ID from Car_MS
    r = requests.post(f"{CAR_MS}/request_car", data=yaml.safe_dump({"need":"car"}), headers={"Content-Type":"application/x-yaml"})
    car_resp = yaml.safe_load(r.content)
    car_id = car_resp.get("car_id")
    log_event({"event":"car_id_received","car_id":car_id, "ts":time.time()})

    # 3. Request current storage for parcel and car (simulate)
    r_parcel = requests.post(f"{STORAGE_MS}/get_parcel", data=yaml.safe_dump({"parcel_id":parcel_id}), headers={"Content-Type":"application/x-yaml"})
    parcel_info = yaml.safe_load(r_parcel.content)
    r_car = requests.post(f"{STORAGE_MS}/get_car", data=yaml.safe_dump({"car_id":car_id}), headers={"Content-Type":"application/x-yaml"})
    car_info = yaml.safe_load(r_car.content)

    # 4. Assign delivery
    assignment = {"parcel_id": parcel_id, "car_id": car_id, "status":"assigned", "assigned_at": time.time()}
    # Share with Storage_MS to store in Database_1
    r_store = requests.post(f"{STORAGE_MS}/store_delivery", data=yaml.safe_dump(assignment), headers={"Content-Type":"application/x-yaml"})
    storage_ack = yaml.safe_load(r_store.content)
    log_event({"event":"delivery_stored","assignment":assignment, "ts":time.time()})

    # 5. Notify Car_MS
    try:
        requests.post(f"{CAR_MS}/notify_assignment", data=yaml.safe_dump(assignment), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[Controller_MS] notify car failed:", e)
    log_event({"event":"car_notified","assignment":assignment, "ts":time.time()})

    # 6. Notify UI_MS then Sender_MS already happens in UI flow
    try:
        requests.post(f"{UI_MS}/notify", data=yaml.safe_dump({"status":"delivery_assigned","assignment":assignment}), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[Controller_MS] notify ui failed:", e)

    return yaml_response({"status":"delivery_assigned","assignment":assignment, "storage_ack":storage_ack})

@app.route("/car_update_request", methods=["POST"])
def car_update_request():
    # Car asking for delivery update
    data = yaml.safe_load(request.data) or {}
    print("[Controller_MS] Car requested update:", data)
    # Acknowledge car
    ack = {"status":"ack","from":"Controller_MS"}
    # Share update with Storage_MS (fetch current then update as example)
    update = {"parcel_id": data.get("parcel_id"), "car_id": data.get("car_id"), "status": data.get("status","in_transit")}
    r = requests.post(f"{STORAGE_MS}/update_delivery", data=yaml.safe_dump(update), headers={"Content-Type":"application/x-yaml"})
    storage_ack = yaml.safe_load(r.content)
    # Notify UI
    try:
        requests.post("http://localhost:5002/notify", data=yaml.safe_dump({"status":"delivery_update","update":update}), headers={"Content-Type":"application/x-yaml"})
    except:
        pass
    log_event({"event":"delivery_update","update":update})
    return yaml_response({"ack": ack, "storage_ack": storage_ack})

if __name__ == "__main__":
    app.run(port=5003, debug=True)

# idgen_ms.py
from flask import Flask, request, Response
import yaml, uuid, requests
import time

app = Flask("IDGen_MS")
STORAGE_MS = "http://localhost:5005"
CONTROLLER_MS = "http://localhost:5003"
LOG_MS = "http://localhost:5007"

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

@app.route("/generate_id", methods=["POST"])
def generate_id():
    data = yaml.safe_load(request.data) or {}
    parcel_id = f"PARCEL-{uuid.uuid4().hex[:8]}"
    # Share with Storage_MS
    try:
        requests.post(f"{STORAGE_MS}/store_parcel_id", data=yaml.safe_dump({"parcel_id":parcel_id}), headers={"Content-Type":"application/x-yaml"})
    except Exception as e:
        print("[IDGen_MS] Storage share failed:", e)
    # Acknowledge Controller_MS implicitly by returning the ID
    # Also inform Log_MS
    try:
        requests.post(f"{LOG_MS}/log", data=yaml.safe_dump({"event":"id_generated","parcel_id":parcel_id,"ts":time.time()}), headers={"Content-Type":"application/x-yaml"})
    except:
        pass
    return yaml_response({"parcel_id": parcel_id})

# log_ms.py
from flask import Flask, request, Response
import yaml, sqlite3, os, time

app = Flask("Log_MS")
DB3 = "db_database_3.sqlite"

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

def init_db():
    if not os.path.exists(DB3):
        conn = sqlite3.connect(DB3)
        c = conn.cursor()
        c.execute("""CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT, payload TEXT, ts REAL)""")
        conn.commit()
        conn.close()
init_db()

@app.route("/log", methods=["POST"])
def log():
    data = yaml.safe_load(request.data) or {}
    event = data.get("event", "<unknown>")
    payload = yaml.safe_dump(data)
    ts = data.get("ts", time.time())
    conn = sqlite3.connect(DB3)
    c = conn.cursor()
    c.execute("INSERT INTO logs(event, payload, ts) VALUES (?, ?, ?)", (event, payload, ts))
    conn.commit()
    conn.close()
    print("[Log_MS] Logged event:", event)
    return yaml_response({"status":"logged","event":event})

# run_sequence.py
import requests, yaml, time

SENDER = "http://localhost:5001"

payload = {
    "from": "test_sender",
    "to": "request_delivery",
    "package": {"weight": "2kg", "description": "Sample parcel"}
}

print("Triggering Sender -> UI -> Controller ...")
r = requests.post(f"{SENDER}/request_delivery", data=yaml.safe_dump(payload), headers={"Content-Type":"application/x-yaml"})
print("Final response from UI via Sender:", yaml.safe_load(r.content))

# sender_ms.py
from flask import Flask, request, Response
import yaml
import requests

app = Flask("Sender_MS")
UI_MS = "http://localhost:5002"  # UI_MS endpoint

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

@app.route("/notify", methods=["POST"])
def notify():
    # Received notification from UI_MS
    data = yaml.safe_load(request.data)
    print("[Sender_MS] Received:", data)
    # Acknowledge
    return yaml_response({"status":"ack", "from":"Sender_MS"})

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    """
    Local endpoint to start a request from Sender to UI
    """
    payload = yaml.safe_load(request.data) if request.data else {}
    print("[Sender_MS] Sending request to UI_MS:", payload)
    r = requests.post(f"{UI_MS}/request_delivery", data=yaml.safe_dump(payload), headers={"Content-Type":"application/x-yaml"})
    resp = yaml.safe_load(r.content)
    return yaml_response({"status":"sent_to_ui", "ui_response": resp})

if __name__ == "__main__":
    app.run(port=5001, debug=True)

# storage_ms.py
from flask import Flask, request, Response
import yaml, sqlite3, os, time

app = Flask("Storage_MS")
DB1 = "db_database_1.sqlite"  # deliveries
DB2 = "db_database_2.sqlite"  # assignments (parcel_id, car_id)
# DB3 for logs is managed by Log_MS

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

def init_db():
    if not os.path.exists(DB1):
        conn = sqlite3.connect(DB1)
        c = conn.cursor()
        c.execute("""CREATE TABLE deliveries (parcel_id TEXT PRIMARY KEY, car_id TEXT, status TEXT, assigned_at REAL)""")
        conn.commit()
        conn.close()
    if not os.path.exists(DB2):
        conn = sqlite3.connect(DB2)
        c = conn.cursor()
        c.execute("""CREATE TABLE assignments (parcel_id TEXT PRIMARY KEY, car_id TEXT, created_at REAL)""")
        conn.commit()
        conn.close()

init_db()

@app.route("/store_parcel_id", methods=["POST"])
def store_parcel_id():
    data = yaml.safe_load(request.data) or {}
    parcel_id = data.get("parcel_id")
    if not parcel_id:
        return yaml_response({"status":"error","msg":"no parcel_id"}, 400)
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    try:
        c.execute("INSERT OR IGNORE INTO assignments(parcel_id, car_id, created_at) VALUES (?, ?, ?)", (parcel_id, None, time.time()))
        conn.commit()
    finally:
        conn.close()
    return yaml_response({"status":"stored_parcel_id","parcel_id":parcel_id})

@app.route("/get_parcel", methods=["POST"])
def get_parcel():
    data = yaml.safe_load(request.data) or {}
    parcel_id = data.get("parcel_id")
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    c.execute("SELECT parcel_id, car_id FROM assignments WHERE parcel_id = ?", (parcel_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return yaml_response({"parcel_id":row[0],"car_id":row[1]})
    else:
        return yaml_response({"status":"not_found"})

@app.route("/store_car_id", methods=["POST"])
def store_car_id():
    data = yaml.safe_load(request.data) or {}
    car_id = data.get("car_id")
    parcel_id = data.get("parcel_id")  # optional association
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    try:
        # If parcel_id present, update that row's car_id
        if parcel_id:
            c.execute("UPDATE assignments SET car_id = ? WHERE parcel_id = ?", (car_id, parcel_id))
        conn.commit()
    finally:
        conn.close()
    return yaml_response({"status":"stored_car_id","car_id":car_id, "parcel_id": parcel_id})

@app.route("/get_car", methods=["POST"])
def get_car():
    data = yaml.safe_load(request.data) or {}
    car_id = data.get("car_id")
    # For this simple demo, we just return provided car_id
    if car_id:
        return yaml_response({"car_id":car_id, "ok":True})
    return yaml_response({"status":"not_found"})

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    data = yaml.safe_load(request.data) or {}
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    status = data.get("status","assigned")
    assigned_at = data.get("assigned_at", time.time())
    if not parcel_id:
        return yaml_response({"status":"error","msg":"no parcel_id"},400)
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    try:
        c.execute("INSERT OR REPLACE INTO deliveries(parcel_id, car_id, status, assigned_at) VALUES (?, ?, ?, ?)", (parcel_id, car_id, status, assigned_at))
        # also reflect in DB2
        c2 = sqlite3.connect("db_database_2.sqlite")
        cc = c2.cursor()
        cc.execute("INSERT OR IGNORE INTO assignments(parcel_id, car_id, created_at) VALUES (?, ?, ?)", (parcel_id, car_id, time.time()))
        cc.execute("UPDATE assignments SET car_id = ? WHERE parcel_id = ?", (car_id, parcel_id))
        c2.commit()
        c2.close()
        conn.commit()
    finally:
        conn.close()
    return yaml_response({"status":"delivery_stored","parcel_id":parcel_id})

@app.route("/update_delivery", methods=["POST"])
def update_delivery():
    data = yaml.safe_load(request.data) or {}
    parcel_id = data.get("parcel_id")
    new_status = data.get("status")
    if not parcel_id:
        return yaml_response({"status":"error","msg":"no parcel_id"},400)
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    try:
        c.execute("UPDATE deliveries SET status = ? WHERE parcel_id = ?", (new_status, parcel_id))
        conn.commit()
    finally:
        conn.close()
    return yaml_response({"status":"updated","parcel_id":parcel_id, "new_status":new_status})

if __name__ == "__main__":
    app.run(port=5005, debug=True)

# ui_ms.py
from flask import Flask, request, Response
import yaml
import requests

app = Flask("UI_MS")
CONTROLLER_MS = "http://localhost:5003"
SENDER_MS = "http://localhost:5001"

def yaml_response(obj, status=200):
    return Response(yaml.safe_dump(obj), status=status, mimetype="application/x-yaml")

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    data = yaml.safe_load(request.data) if request.data else {}
    print("[UI_MS] Received request_delivery from Sender_MS:", data)
    # Forward to Controller_MS
    forward = {"action":"request_delivery","sender_data":data}
    r = requests.post(f"{CONTROLLER_MS}/request_delivery", data=yaml.safe_dump(forward), headers={"Content-Type":"application/x-yaml"})
    controller_resp = yaml.safe_load(r.content)
    # notify sender to acknowledge
    try:
        requests.post(f"{SENDER_MS}/notify", data=yaml.safe_dump({"status":"notified_sender"}), headers={"Content-Type":"application/x-yaml"}, timeout=2)
    except Exception as e:
        print("[UI_MS] Warning: couldn't notify Sender_MS:", e)
    return yaml_response({"status":"forwarded_to_controller", "controller": controller_resp})

@app.route("/notify", methods=["POST"])
def notify():
    data = yaml.safe_load(request.data)
    print("[UI_MS] Notification received (from Controller or others):", data)
    # Acknowledge to the notifier (Controller usually)
    return yaml_response({"status":"ack", "from":"UI_MS"})

if __name__ == "__main__":
    app.run(port=5002, debug=True)

