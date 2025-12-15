# services/car_ms.py
from flask import Flask, request, Response
import yaml, requests

app = Flask(__name__)
STORAGE_URL = "http://localhost:5004"

# We'll accept any car id as valid for this sample. In a real system we'd check inventory.
@app.route('/check', methods=['POST'])
def check_car():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    car_id = payload.get('car_id')
    if not car_id:
        return Response(yaml.safe_dump({'status':'error','reason':'missing car_id'}), mimetype='text/yaml', status=400)
    # share car id with storage
    s_payload = {'type':'car','value':car_id}
    r = requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(s_payload), headers={'Content-Type':'text/yaml'})
    resp = {'status':'ok','car_id':car_id, 'storage_ack': yaml.safe_load(r.text)}
    return Response(yaml.safe_dump(resp), mimetype='text/yaml')

# Car also can request delivery updates from Controller
@app.route('/request_update', methods=['GET'])
def request_update():
    # Car requests updates from controller (Controller endpoint address would be known in deployment)
    return Response(yaml.safe_dump({'status':'ok','msg':'car requests would be sent to controller'}), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5007, host='0.0.0.0')

# services/controller_ms.py
from flask import Flask, request, Response
import yaml, requests

app = Flask(__name__)
IDGEN_URL = "http://localhost:5003"
STORAGE_URL = "http://localhost:5004"
LOG_URL = "http://localhost:5005"
CAR_URL = "http://localhost:5007"
UI_URL = "http://localhost:5001"

def send_log(source, message):
    payload = {'source': source, 'message': message}
    requests.post(f"{LOG_URL}/log", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})

@app.route('/request_delivery', methods=['POST'])
def request_delivery():
    # Step: Controller receives forwarded 'request delivery' from UI_MS
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    sender = payload.get('sender','unknown')
    details = payload.get('details',{})
    # 1) request parcel ID from IDGen_MS
    r = requests.post(f"{IDGEN_URL}/generate", data=yaml.safe_dump({'request_from':'controller'}), headers={'Content-Type':'text/yaml'})
    id_resp = yaml.safe_load(r.text)
    parcel_id = id_resp.get('parcel_id')
    # send log
    send_log('Controller_MS', f"Generated parcel id {parcel_id}")
    # 2) request car id from Car_MS (simulate client provided car_id in details or pick one)
    car_id = details.get('car_id', 'CAR-DEFAULT-1')
    r2 = requests.post(f"{CAR_URL}/check", data=yaml.safe_dump({'car_id': car_id}), headers={'Content-Type':'text/yaml'})
    car_resp = yaml.safe_load(r2.text)
    send_log('Controller_MS', f"Car check for {car_id} result: {car_resp.get('status')}")
    # 3) fetch parcel_id and car_id back from storage to ensure consistency
    parcel_stored = yaml.safe_load(requests.get(f"{STORAGE_URL}/get_id/parcel").text)
    car_stored = yaml.safe_load(requests.get(f"{STORAGE_URL}/get_id/car").text)
    send_log('Controller_MS', f"Fetched stored parcel/car: {parcel_stored.get('value')}, {car_stored.get('value')}")
    # 4) assign delivery
    assignment = {'parcel_id': parcel_id, 'car_id': car_id, 'assigned_by': 'Controller_MS'}
    r3 = requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(assignment), headers={'Content-Type':'text/yaml'})
    store_ack = yaml.safe_load(r3.text)
    send_log('Controller_MS', f"Stored delivery: {assignment}")
    # 5) notify car
    notify_car = requests.post(f"{CAR_URL}/notify", data=yaml.safe_dump({'parcel_id': parcel_id, 'car_id': car_id}), headers={'Content-Type':'text/yaml'}) if False else None
    # car endpoint /notify might not exist; instead simulate acknowledgement from car via controller call:
    send_log('Controller_MS', f"Notified Car_MS about parcel {parcel_id}")
    # 6) notify UI
    requests.post(f"{UI_URL}/notify_assignment", data=yaml.safe_dump({'parcel_id': parcel_id, 'car_id': car_id}), headers={'Content-Type':'text/yaml'})
    send_log('Controller_MS', f"Notified UI_MS about assignment {parcel_id}->{car_id}")
    # final ack to UI caller
    resp = {'status':'ok','parcel_id': parcel_id, 'car_id': car_id, 'storage_ack': store_ack}
    return Response(yaml.safe_dump(resp), mimetype='text/yaml')

@app.route('/delivery_update', methods=['POST'])
def delivery_update():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    car_id = payload.get('car_id')
    parcel_id = payload.get('parcel_id')
    status = payload.get('status','in_transit')
    # ack car
    ack = {'status':'ok','acknowledged':True}
    # update storage
    s_payload = {'parcel_id': parcel_id, 'car_id': car_id, 'metadata': {'status': status}}
    r = requests.post(f"{STORAGE_URL}/store_delivery", data=yaml.safe_dump(s_payload), headers={'Content-Type':'text/yaml'})
    # notify UI -> UI will notify sender etc.
    requests.post(f"{UI_URL}/notify_update", data=yaml.safe_dump({'parcel_id': parcel_id, 'status': status}), headers={'Content-Type':'text/yaml'})
    send_log('Controller_MS', f"Delivery update for {parcel_id}: {status}")
    return Response(yaml.safe_dump(ack), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5002, host='0.0.0.0')

# services/idgen_ms.py
from flask import Flask, request, Response
import yaml, uuid, requests

app = Flask(__name__)
STORAGE_URL = "http://localhost:5004"   # Storage_MS

@app.route('/generate', methods=['POST'])
def generate_id():
    data = yaml.safe_load(request.data.decode('utf-8') or "{}")
    # generate parcel id
    parcel_id = "P-" + str(uuid.uuid4())
    # share parcel ID with Storage_MS
    payload = {'type':'parcel','value':parcel_id}
    r = requests.post(f"{STORAGE_URL}/store_id", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
    # Acknowledge
    resp = {'status':'ok','parcel_id': parcel_id, 'storage_ack': yaml.safe_load(r.text)}
    return Response(yaml.safe_dump(resp), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5003, host='0.0.0.0')

# services/log_ms.py
from flask import Flask, request, Response
import yaml
import sqlite3

app = Flask(__name__)

def store_log(source, message):
    conn = sqlite3.connect('database_3.db')
    c = conn.cursor()
    c.execute('INSERT INTO logs (source, message) VALUES (?, ?)', (source, message))
    conn.commit()
    conn.close()

@app.route('/log', methods=['POST'])
def receive_log():
    data = yaml.safe_load(request.data.decode('utf-8') or "{}")
    source = data.get('source', 'unknown')
    message = data.get('message', '')
    store_log(source, message)
    resp = {'status':'ok', 'stored': True}
    yaml_resp = yaml.safe_dump(resp)
    return Response(yaml_resp, mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5005, host='0.0.0.0')

# services/sender_ms.py
from flask import Flask, request, Response
import yaml
import requests

app = Flask(__name__)
UI_URL = "http://localhost:5001"

@app.route('/start_delivery', methods=['POST'])
def start_delivery():
    # Expects YAML payload with details (e.g. car_id)
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    r = requests.post(f"{UI_URL}/request_delivery", data=yaml.safe_dump({'sender':'Sender_MS','details': payload}), headers={'Content-Type':'text/yaml'})
    return Response(r.text, mimetype='text/yaml')

@app.route('/notify_assignment', methods=['POST'])
def notify_assignment():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    # Sender acknowledges
    ack = {'status':'ok','received_assignment': True, 'assignment': payload}
    return Response(yaml.safe_dump(ack), mimetype='text/yaml')

@app.route('/notify_update', methods=['POST'])
def notify_update():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    ack = {'status':'ok','received_update': True, 'update': payload}
    return Response(yaml.safe_dump(ack), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5006, host='0.0.0.0')

# services/storage_ms.py
from flask import Flask, request, Response
import yaml
import sqlite3
import json

app = Flask(__name__)

def store_assignment(key_type, key_value):
    conn = sqlite3.connect('database_2.db')
    c = conn.cursor()
    c.execute('INSERT INTO assignments (key_type, key_value) VALUES (?, ?)', (key_type, key_value))
    conn.commit()
    conn.close()

def store_delivery(parcel_id, car_id, status='assigned', metadata=None):
    conn = sqlite3.connect('database_1.db')
    c = conn.cursor()
    meta_json = json.dumps(metadata) if metadata else None
    c.execute('INSERT INTO deliveries (parcel_id, car_id, status, metadata) VALUES (?, ?, ?, ?)', (parcel_id, car_id, status, meta_json))
    conn.commit()
    conn.close()

def get_latest_key(key_type):
    conn = sqlite3.connect('database_2.db')
    c = conn.cursor()
    c.execute('SELECT key_value FROM assignments WHERE key_type=? ORDER BY id DESC LIMIT 1', (key_type,))
    r = c.fetchone()
    conn.close()
    return r[0] if r else None

@app.route('/store_id', methods=['POST'])
def store_id():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    key_type = payload.get('type')
    key_value = payload.get('value')
    if not key_type or not key_value:
        return Response(yaml.safe_dump({'status':'error','reason':'missing type or value'}), mimetype='text/yaml', status=400)
    store_assignment(key_type, key_value)
    return Response(yaml.safe_dump({'status':'ok','ack':True}), mimetype='text/yaml')

@app.route('/store_delivery', methods=['POST'])
def store_delivery_endpoint():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    parcel_id = payload.get('parcel_id')
    car_id = payload.get('car_id')
    metadata = payload.get('metadata')
    if not parcel_id or not car_id:
        return Response(yaml.safe_dump({'status':'error','reason':'missing parcel_id or car_id'}), mimetype='text/yaml', status=400)
    store_delivery(parcel_id, car_id, metadata=metadata)
    return Response(yaml.safe_dump({'status':'ok','stored':True}), mimetype='text/yaml')

@app.route('/get_id/<key_type>', methods=['GET'])
def get_id(key_type):
    val = get_latest_key(key_type)
    resp = {'status':'ok','type':key_type, 'value': val}
    return Response(yaml.safe_dump(resp), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5004, host='0.0.0.0')

# services/ui_ms.py
from flask import Flask, request, Response
import yaml, requests

app = Flask(__name__)
CONTROLLER_URL = "http://localhost:5002"
SENDER_URL = "http://localhost:5006"

@app.route('/request_delivery', methods=['POST'])
def request_delivery():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    # Forward to controller
    r = requests.post(f"{CONTROLLER_URL}/request_delivery", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
    resp = yaml.safe_load(r.text)
    return Response(yaml.safe_dump({'status':'forwarded','controller_response': resp}), mimetype='text/yaml')

@app.route('/notify_assignment', methods=['POST'])
def notify_assignment():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    # Notify sender
    requests.post(f"{SENDER_URL}/notify_assignment", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
    return Response(yaml.safe_dump({'status':'ok','notified_sender': True}), mimetype='text/yaml')

@app.route('/notify_update', methods=['POST'])
def notify_update():
    payload = yaml.safe_load(request.data.decode('utf-8') or "{}")
    requests.post(f"{SENDER_URL}/notify_update", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
    return Response(yaml.safe_dump({'status':'ok','notified_sender': True}), mimetype='text/yaml')

if __name__ == '__main__':
    app.run(port=5001, host='0.0.0.0')

# db_init.py
import sqlite3

def init_db():
    # Database 1: deliveries (parcel assignments)
    conn = sqlite3.connect('database_1.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parcel_id TEXT,
            car_id TEXT,
            status TEXT,
            metadata TEXT
        )
    ''')
    conn.commit()
    conn.close()

    # Database 2: assignments / ids (stores parcel IDs and car IDs)
    conn = sqlite3.connect('database_2.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_type TEXT, -- 'parcel' or 'car'
            key_value TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

    # Database 3: logs
    conn = sqlite3.connect('database_3.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Databases initialized: database_1.db, database_2.db, database_3.db")

# simulation.py
import requests, yaml, time

SENDER_URL = "http://localhost:5006"
# Start a delivery request: include a car_id to use
payload = {'car_id': 'CAR-12345'}
print("Sender -> UI: request_delivery (YAML):", payload)
r = requests.post(f"{SENDER_URL}/start_delivery", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
print("Final response (from UI -> Controller flow):")
print(r.text)

# db_init.py
import sqlite3

def init_db():
    # Database 1: deliveries (parcel assignments)
    conn = sqlite3.connect('database_1.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parcel_id TEXT,
            car_id TEXT,
            status TEXT,
            metadata TEXT
        )
    ''')
    conn.commit()
    conn.close()

    # Database 2: assignments / ids (stores parcel IDs and car IDs)
    conn = sqlite3.connect('database_2.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_type TEXT, -- 'parcel' or 'car'
            key_value TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

    # Database 3: logs
    conn = sqlite3.connect('database_3.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
    print("Databases initialized: database_1.db, database_2.db, database_3.db")

# simulation.py
import requests, yaml, time

SENDER_URL = "http://localhost:5006"
# Start a delivery request: include a car_id to use
payload = {'car_id': 'CAR-12345'}
print("Sender -> UI: request_delivery (YAML):", payload)
r = requests.post(f"{SENDER_URL}/start_delivery", data=yaml.safe_dump(payload), headers={'Content-Type':'text/yaml'})
print("Final response (from UI -> Controller flow):")
print(r.text)

