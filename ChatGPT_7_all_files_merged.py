# car.py
from flask import Flask, request, Response
import yaml, requests
import yaml as y

conf = y.safe_load(open("config.yaml"))
CONTROLLER_URL = f"http://{conf['controller_host']}:{conf['controller_port']}"
STORAGE_URL = f"http://{conf['storage_host']}:{conf['storage_port']}"

app = Flask("Car_MS")

# For demo, a simple in-memory set of valid car ids
VALID_CARS = {"CAR-100","CAR-200","CAR-300"}

@app.route("/check_car", methods=["POST"])
def check_car():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    car_id = data.get("car_id")
    if car_id in VALID_CARS:
        # store in storage: key car:{car_id} -> car_id
        payload = {"key": f"car:{car_id}", "value": car_id}
        headers = {"Content-Type":"application/x-yaml"}
        r = requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(payload), headers=headers)
        if r.status_code == 200:
            # ack controller
            return Response(yaml.safe_dump({"status":"ok","car_id":car_id}), mimetype="application/x-yaml")
    return Response(yaml.safe_dump({"status":"not_found"}), mimetype="application/x-yaml", status=404)

@app.route("/notify_assignment", methods=["POST"])
def notify_assignment():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    # Car would process assignment (simulate ack)
    return Response(yaml.safe_dump({"status":"ack"}), mimetype="application/x-yaml")

@app.route("/request_update", methods=["POST"])
def request_update():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    # Car asks Controller for update about a parcel_id
    parcel_id = data.get("parcel_id")
    headers = {"Content-Type":"application/x-yaml"}
    r = requests.post(f"{CONTROLLER_URL}/car_request_update", data=yaml.safe_dump({"car_id":data.get("car_id"), "parcel_id": parcel_id}), headers=headers)
    return Response(r.content, mimetype="application/x-yaml", status=r.status_code)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5200)

# controller.py
from flask import Flask, request, Response
import yaml, requests
import yaml as y

conf = y.safe_load(open("config.yaml"))
IDGEN_URL = f"http://{conf['idgen_host']}:{conf['idgen_port']}"
STORAGE_URL = f"http://{conf['storage_host']}:{conf['storage_port']}"
LOG_URL = f"http://{conf['log_host']}:{conf['log_port']}"
CAR_URL = f"http://{conf['car_host']}:{conf['car_port']}"
UI_URL = f"http://{conf['ui_host']}:{conf['ui_port']}"

app = Flask("Controller_MS")

def send_log(source, event, payload):
    headers = {"Content-Type":"application/x-yaml"}
    log_payload = {"source": source, "event": event, "payload": payload}
    requests.post(f"{LOG_URL}/log", data=yaml.safe_dump(log_payload), headers=headers)

@app.route("/handle_request_delivery", methods=["POST"])
def handle_request_delivery():
    # UI forwards 'request delivery' to controller
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    req = yaml.safe_load(request.data)
    sender_info = req.get("sender")
    parcel_meta = req.get("parcel", {})
    headers = {"Content-Type":"application/x-yaml"}

    # 1) Request parcel ID from IDGen_MS
    r = requests.post(f"{IDGEN_URL}/request_id", data=yaml.safe_dump({"purpose":"parcel"}), headers=headers)
    if r.status_code != 200:
        send_log("Controller_MS","idgen_failed", {"reason": r.text})
        return Response(yaml.safe_dump({"status":"error","reason":"idgen_failed"}), mimetype="application/x-yaml", status=500)
    resp = yaml.safe_load(r.content)
    parcel_id = resp["parcel_id"]

    send_log("Controller_MS","parcel_id_received", {"parcel_id":parcel_id})

    # 2) Request car id from Car_MS (here we pick one candidate; for demo we pass a candidate car_id)
    candidate_car = req.get("preferred_car","CAR-100")
    r2 = requests.post(f"{CAR_URL}/check_car", data=yaml.safe_dump({"car_id":candidate_car}), headers=headers)
    if r2.status_code != 200:
        send_log("Controller_MS","car_check_failed", {"car_id": candidate_car})
        return Response(yaml.safe_dump({"status":"error","reason":"car_check_failed"}), mimetype="application/x-yaml", status=500)
    car_resp = yaml.safe_load(r2.content)
    car_id = car_resp.get("car_id")
    send_log("Controller_MS","car_id_received", {"car_id":car_id})

    # 3) retrieve stored parcel & car ids via storage (simulating get)
    gp = requests.get(f"{STORAGE_URL}/get_id/parcel:{parcel_id}")
    gc = requests.get(f"{STORAGE_URL}/get_id/car:{car_id}")
    if gp.status_code != 200 or gc.status_code != 200:
        send_log("Controller_MS","storage_missing_ids", {"parcel_get":gp.status_code, "car_get":gc.status_code})
        return Response(yaml.safe_dump({"status":"error","reason":"storage_missing_ids"}), mimetype="application/x-yaml", status=500)

    # 4) assign delivery (store into database_1 via Storage_MS)
    assign_payload = {"parcel_id": parcel_id, "car_id": car_id, "status":"assigned", "metadata": parcel_meta}
    rs = requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(assign_payload), headers=headers)
    if rs.status_code != 200:
        send_log("Controller_MS","store_delivery_failed", {"parcel_id":parcel_id})
        return Response(yaml.safe_dump({"status":"error","reason":"store_failed"}), mimetype="application/x-yaml", status=500)
    send_log("Controller_MS","delivery_assigned", {"parcel_id":parcel_id,"car_id":car_id})

    # 5) Notify Car_MS
    notify_payload = {"parcel_id": parcel_id, "car_id": car_id, "action":"assign"}
    rcar = requests.post(f"{CAR_URL}/notify_assignment", data=yaml.safe_dump(notify_payload), headers=headers)
    # Car acknowledges
    if rcar.status_code == 200:
        send_log("Controller_MS","car_ack", {"parcel_id":parcel_id})
    else:
        send_log("Controller_MS","car_no_ack", {"parcel_id":parcel_id})

    # 6) Notify UI/MS (so UI can notify Sender)
    ui_notify = {"parcel_id":parcel_id,"status":"assigned","car_id":car_id}
    rui = requests.post(f"{UI_URL}/notify", data=yaml.safe_dump(ui_notify), headers=headers)
    # UI will call Sender and ack back (UI responds back to us)
    send_log("Controller_MS","ui_notified", {"resp_status": rui.status_code})

    # Final controller ack to UI (the UI forwarded controller's response to the original Sender)
    return Response(yaml.safe_dump({"status":"ok","parcel_id":parcel_id,"car_id":car_id}), mimetype="application/x-yaml")

@app.route("/car_request_update", methods=["POST"])
def car_request_update():
    # Car asks Controller for a delivery update
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    headers = {"Content-Type":"application/x-yaml"}
    # For simplicity, acknowledge to car immediately
    send_log("Controller_MS","car_requested_update", {"parcel_id":parcel_id,"car_id":car_id})
    # send update to storage to change status if requested (here we simulate updating status to 'in_transit')
    update_payload = {"parcel_id": parcel_id, "update":{"status":"in_transit"}}
    r = requests.post(f"{STORAGE_URL}/update_delivery", data=yaml.safe_dump(update_payload), headers=headers)
    send_log("Controller_MS","storage_updated_for_car", {"parcel_id":parcel_id})
    # notify UI & sender
    ui_notify = {"parcel_id":parcel_id,"status":"in_transit","car_id":car_id}
    requests.post(f"{UI_URL}/notify", data=yaml.safe_dump(ui_notify), headers=headers)
    send_log("Controller_MS","ui_notified_of_update", {"parcel_id":parcel_id})
    return Response(yaml.safe_dump({"status":"ok"}), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

# idgen.py
from flask import Flask, request, Response
import yaml, uuid, requests
import configparser, os
import yaml as y

# read config.yaml for Storage endpoint
conf = y.safe_load(open("config.yaml"))

STORAGE_URL = f"http://{conf['storage_host']}:{conf['storage_port']}"

app = Flask("IDGen_MS")

@app.route("/request_id", methods=["POST"])
def request_id():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    purpose = data.get("purpose","parcel")
    # generate ID
    parcel_id = f"P-{uuid.uuid4().hex[:8]}"
    # send to storage: store_id {key: parcel:{parcel_id}, value: parcel_id}
    payload = {"key": f"parcel:{parcel_id}", "value": parcel_id}
    headers = {"Content-Type":"application/x-yaml"}
    r = requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(payload), headers=headers)
    # wait for storage ack (we assume HTTP ok)
    if r.status_code == 200:
        # reply to controller with the parcel id
        return Response(yaml.safe_dump({"status":"ok","parcel_id":parcel_id}), mimetype="application/x-yaml")
    else:
        return Response(yaml.safe_dump({"status":"error","reason":"storage_failed"}), mimetype="application/x-yaml", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)

# log_ms.py
from flask import Flask, request, Response
import yaml
import sqlite3
import os

DB_FILE = "database_3_logs.sqlite"

app = Flask("Log_MS")

def init_db():
    if not os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""CREATE TABLE logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            event TEXT,
            payload TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()
        conn.close()

@app.route("/log", methods=["POST"])
def log_event():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    source = data.get("source")
    event = data.get("event")
    payload = yaml.safe_dump(data.get("payload", {}))
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO logs (source,event,payload) VALUES (?,?,?)", (source, event, payload))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status":"ok"}), mimetype="application/x-yaml")

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5004)

# sender.py
from flask import Flask, request, Response
import yaml, requests
import yaml as y

conf = y.safe_load(open("config.yaml"))
UI_URL = f"http://{conf['ui_host']}:{conf['ui_port']}"

app = Flask("Sender_MS")

@app.route("/start_send", methods=["POST"])
def start_send():
    # Sender requests delivery via UI_MS
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    headers = {"Content-Type":"application/x-yaml"}
    r = requests.post(f"{UI_URL}/request_delivery", data=yaml.safe_dump(data), headers=headers)
    return Response(r.content, mimetype="application/x-yaml", status=r.status_code)

@app.route("/notify", methods=["POST"])
def notify():
    # UI notifies Sender
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    # Sender acknowledges
    return Response(yaml.safe_dump({"status":"ack_from_sender"}), mimetype="application/x-yaml")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5100)

# storage.py
from flask import Flask, request, Response
import yaml, sqlite3, os

DB1 = "database_1_parcels.sqlite"     # stores deliveries / parcels
DB2 = "database_2_assignments.sqlite" # stores parcel id and car id assignments

app = Flask("Storage_MS")

def init_db():
    if not os.path.exists(DB1):
        conn = sqlite3.connect(DB1)
        c = conn.cursor()
        c.execute("""CREATE TABLE deliveries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parcel_id TEXT UNIQUE,
            car_id TEXT,
            status TEXT,
            metadata TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()
        conn.close()
    if not os.path.exists(DB2):
        conn = sqlite3.connect(DB2)
        c = conn.cursor()
        c.execute("""CREATE TABLE assignments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE,
            value TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()
        conn.close()

@app.route("/store_id", methods=["POST"])
def store_id():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    key = data["key"]
    value = data["value"]
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO assignments (key,value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status":"ok"}), mimetype="application/x-yaml")

@app.route("/get_id/<key>", methods=["GET"])
def get_id(key):
    conn = sqlite3.connect(DB2)
    c = conn.cursor()
    c.execute("SELECT value FROM assignments WHERE key=?", (key,))
    row = c.fetchone()
    conn.close()
    if not row:
        return Response(yaml.safe_dump({"status":"not_found"}), mimetype="application/x-yaml", status=404)
    return Response(yaml.safe_dump({"status":"ok","value":row[0]}), mimetype="application/x-yaml")

@app.route("/store_delivery", methods=["POST"])
def store_delivery():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    parcel_id = data["parcel_id"]
    car_id = data.get("car_id")
    status = data.get("status","assigned")
    metadata = yaml.safe_dump(data.get("metadata", {}))
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO deliveries (parcel_id,car_id,status,metadata) VALUES (?,?,?,?)",
              (parcel_id, car_id, status, metadata))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status":"ok"}), mimetype="application/x-yaml")

@app.route("/update_delivery", methods=["POST"])
def update_delivery():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    parcel_id = data["parcel_id"]
    update_fields = data.get("update", {})  # e.g. {"status":"delivered"}
    conn = sqlite3.connect(DB1)
    c = conn.cursor()
    # simple support for status and car_id
    if "status" in update_fields:
        c.execute("UPDATE deliveries SET status=? WHERE parcel_id=?", (update_fields["status"], parcel_id))
    if "car_id" in update_fields:
        c.execute("UPDATE deliveries SET car_id=? WHERE parcel_id=?", (update_fields["car_id"], parcel_id))
    conn.commit()
    conn.close()
    return Response(yaml.safe_dump({"status":"ok"}), mimetype="application/x-yaml")

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5003)

# ui.py
from flask import Flask, request, Response
import yaml, requests
import yaml as y

conf = y.safe_load(open("config.yaml"))
CONTROLLER_URL = f"http://{conf['controller_host']}:{conf['controller_port']}"
SENDER_URL = f"http://{conf['sender_host']}:{conf['sender_port']}"

app = Flask("UI_MS")

@app.route("/request_delivery", methods=["POST"])
def request_delivery():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    # forward to controller
    headers = {"Content-Type":"application/x-yaml"}
    r = requests.post(f"{CONTROLLER_URL}/handle_request_delivery", data=yaml.safe_dump(data), headers=headers)
    # forward controller's response back to sender (simulate notify)
    return Response(r.content, mimetype="application/x-yaml", status=r.status_code)

@app.route("/notify", methods=["POST"])
def notify_sender():
    if request.content_type != "application/x-yaml":
        return Response("Unsupported content type", status=415)
    data = yaml.safe_load(request.data)
    # this endpoint is used to notify the sender in the flow (UI->Sender)
    headers = {"Content-Type":"application/x-yaml"}
    # send to Sender and return ack back to controller when done
    r = requests.post(f"{SENDER_URL}/notify", data=yaml.safe_dump(data), headers=headers)
    return Response(r.content, mimetype="application/x-yaml", status=r.status_code)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

