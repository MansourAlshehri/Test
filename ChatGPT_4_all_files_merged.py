# car_ms/app.py
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import httpx

app = FastAPI(title="Car_MS")

CAR_ID = "CAR-ALPHA-001"

# CONFIG: Storage and Controller URLs
STORAGE_URL = "http://server1:8003"
CONTROLLER_URL = "http://server1:8001"

@app.post("/request_car")
async def request_car(request: Request):
    data = await read_yaml(request)
    # Car checks its ID (some check)
    valid = True
    import yaml
    async with httpx.AsyncClient() as client:
        # share car id with Storage_MS
        await client.post(f"{STORAGE_URL}/store_car_id", content=yaml.safe_dump({"car_id": CAR_ID}), headers={"content-type":"application/x-yaml"})
    # after storing, acknowledge Controller by returning car_id
    return yaml_response({"car_id": CAR_ID, "status":"shared_with_storage"})

# endpoint to receive notification from Controller when assigned
@app.post("/notify_assignment")
async def notify_assignment(request: Request):
    data = await read_yaml(request)
    # acknowledge the controller
    return yaml_response({"status":"ack","received":data})

# endpoint for the car to request updates from Controller (simulate)
@app.post("/request_delivery_update")
async def request_delivery_update(request: Request):
    data = await read_yaml(request)
    # forward to Controller (simulate)
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{CONTROLLER_URL}/car_update_request", content=yaml.safe_dump({"car_id":CAR_ID}), headers={"content-type":"application/x-yaml"})
    return yaml_response({"status":"requested_update","controller_response": res.text})

# controller_ms/app.py
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import httpx
import uuid
import yaml

app = FastAPI(title="Controller_MS")

# CONFIG - set real hostnames/ports as needed
IDGEN_URL = "http://server1:8004"
STORAGE_URL = "http://server1:8003"
CAR_URL = "http://laptop1:8005"
LOG_URL = "http://server1:8006"
UI_URL = "http://server1:8002"

async def send_log(source, message):
    async with httpx.AsyncClient() as client:
        await client.post(f"{LOG_URL}/store_log", content=yaml.safe_dump({"source":source,"message":message}), headers={"content-type":"application/x-yaml"})

@app.post("/process_request")
async def process_request(request: Request):
    data = await read_yaml(request)
    # 1) request parcel ID from IDGen_MS
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{IDGEN_URL}/generate_id", content=yaml.safe_dump({}), headers={"content-type":"application/x-yaml"})
        parcel_resp = yaml.safe_load(res.text)
        parcel_id = parcel_resp.get("parcel_id")

    await send_log("Controller_MS", f"Received parcel_id {parcel_id}")

    # 2) request car from Car_MS
    async with httpx.AsyncClient() as client:
        res = await client.post(f"{CAR_URL}/request_car", content=yaml.safe_dump({}), headers={"content-type":"application/x-yaml"})
        car_resp = yaml.safe_load(res.text)
        car_id = car_resp.get("car_id")

    await send_log("Controller_MS", f"Received car_id {car_id}")

    # 3) ask Storage for stored parcel and car IDs (as specified)
    async with httpx.AsyncClient() as client:
        res_parcel = await client.post(f"{STORAGE_URL}/get_parcel_id", content=yaml.safe_dump({}), headers={"content-type":"application/x-yaml"})
        parcel_stored = yaml.safe_load(res_parcel.text).get("parcel_id")
        res_car = await client.post(f"{STORAGE_URL}/get_car_id", content=yaml.safe_dump({}), headers={"content-type":"application/x-yaml"})
        car_stored = yaml.safe_load(res_car.text).get("car_id")

    # 4) assign delivery
    delivery_id = str(uuid.uuid4())
    delivery = {"delivery_id": delivery_id, "parcel_id": parcel_stored, "car_id": car_stored, "status":"assigned"}
    async with httpx.AsyncClient() as client:
        await client.post(f"{STORAGE_URL}/store_delivery", content=yaml.safe_dump(delivery), headers={"content-type":"application/x-yaml"})
    await send_log("Controller_MS", f"Assigned delivery {delivery_id}")

    # 5) notify car
    async with httpx.AsyncClient() as client:
        await client.post(f"{CAR_URL}/notify_assignment", content=yaml.safe_dump(delivery), headers={"content-type":"application/x-yaml"})
    await send_log("Controller_MS", f"Notified Car {car_stored} of delivery {delivery_id}")

    # 6) notify UI and indirectly Sender
    async with httpx.AsyncClient() as client:
        await client.post(f"{UI_URL}/notify_sender", content=yaml.safe_dump({"delivery_id":delivery_id,"parcel_id":parcel_stored,"car_id":car_stored}), headers={"content-type":"application/x-yaml"})

    await send_log("Controller_MS", f"Notified UI of delivery {delivery_id}")

    return yaml_response({"status":"ok","delivery_id":delivery_id})

# endpoint Car calls to request updates
@app.post("/car_update_request")
async def car_update(request: Request):
    data = await read_yaml(request)
    car_id = data.get("car_id")
    # ack car
    await send_log("Controller_MS", f"Car {car_id} requested update")
    # share update with Storage_MS
    async with httpx.AsyncClient() as client:
        await client.post(f"{STORAGE_URL}/update_delivery", content=yaml.safe_dump({"delivery_id": data.get("delivery_id"), "status": data.get("status","in_transit"), "info": data.get("info","")}), headers={"content-type":"application/x-yaml"})
    await send_log("Controller_MS", f"Updated storage for car {car_id}")
    # notify UI
    async with httpx.AsyncClient() as client:
        await client.post(f"{UI_URL}/notify_update", content=yaml.safe_dump({"car_id":car_id, "status":"update_sent"}), headers={"content-type":"application/x-yaml"})
    return yaml_response({"status":"ok","ack":"car_update_handled"})

# log_ms/app.py
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import aiosqlite
import os

app = FastAPI(title="Log_MS")
DB3 = "database_3_logs.sqlite"

@app.on_event("startup")
async def startup():
    async with aiosqlite.connect(DB3) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            message TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.commit()

@app.post("/store_log")
async def store_log(request: Request):
    data = await read_yaml(request)
    source = data.get("source")
    message = data.get("message")
    async with aiosqlite.connect(DB3) as db:
        await db.execute("INSERT INTO logs (source, message) VALUES (?, ?)", (source, message))
        await db.commit()
    return yaml_response({"status":"ok","stored":"log"})

# sender_ms/app.py
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import httpx
import yaml

app = FastAPI(title="Sender_MS")

UI_URL = "http://server1:8002"

@app.post("/start_request")
async def start_request(request: Request):
    data = await read_yaml(request)
    # send to UI_MS
    async with httpx.AsyncClient() as client:
        await client.post(f"{UI_URL}/request_delivery", content=yaml.safe_dump(data), headers={"content-type":"application/x-yaml"})
    return yaml_response({"status":"sent_to_ui"})

# callback endpoint that UI uses to notify Sender_MS
@app.post("/ack")
async def ack(request: Request):
    data = await read_yaml(request)
    # Sender acknowledges UI
    return yaml_response({"status":"ack_received","data":data})

# storage_ms/app.py
import asyncio
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import aiosqlite
import os

app = FastAPI(title="Storage_MS")

DB1 = "database_1_parcels.sqlite"   # parcel data (deliveries)
DB2 = "database_2_assignments.sqlite" # parcel id <-> car id

async def init_db():
    async with aiosqlite.connect(DB1) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id TEXT PRIMARY KEY,
            parcel_id TEXT,
            car_id TEXT,
            status TEXT,
            info TEXT
        )""")
        await db.commit()

    async with aiosqlite.connect(DB2) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parcel_id TEXT,
            car_id TEXT
        )""")
        await db.commit()

@app.on_event("startup")
async def startup():
    await init_db()

@app.post("/store_parcel_id")
async def store_parcel_id(request: Request):
    data = await read_yaml(request)
    parcel_id = data.get("parcel_id")
    # store in assignments (DB2) with car_id NULL for now
    async with aiosqlite.connect(DB2) as db:
        await db.execute("INSERT INTO assignments (parcel_id, car_id) VALUES (?, ?)", (parcel_id, None))
        await db.commit()
    return yaml_response({"status":"ok","stored":"parcel_id"})

@app.post("/store_car_id")
async def store_car_id(request: Request):
    data = await read_yaml(request)
    car_id = data.get("car_id")
    parcel_id = data.get("parcel_id")  # optional association
    async with aiosqlite.connect(DB2) as db:
        if parcel_id:
            await db.execute("INSERT INTO assignments (parcel_id, car_id) VALUES (?, ?)", (parcel_id, car_id))
        else:
            await db.execute("INSERT INTO assignments (parcel_id, car_id) VALUES (?, ?)", (None, car_id))
        await db.commit()
    return yaml_response({"status":"ok","stored":"car_id"})

@app.post("/store_delivery")
async def store_delivery(request: Request):
    data = await read_yaml(request)
    delivery_id = data.get("delivery_id")
    parcel_id = data.get("parcel_id")
    car_id = data.get("car_id")
    status = data.get("status","assigned")
    info = data.get("info","")
    async with aiosqlite.connect(DB1) as db:
        await db.execute("INSERT INTO deliveries (delivery_id, parcel_id, car_id, status, info) VALUES (?, ?, ?, ?, ?)",
                         (delivery_id, parcel_id, car_id, status, info))
        await db.commit()
    return yaml_response({"status":"ok","stored":"delivery"})

@app.post("/get_parcel_id")
async def get_parcel_id(request: Request):
    data = await read_yaml(request)
    # return the most recent parcel_id
    async with aiosqlite.connect(DB2) as db:
        async with db.execute("SELECT parcel_id FROM assignments WHERE parcel_id IS NOT NULL ORDER BY id DESC LIMIT 1") as cur:
            row = await cur.fetchone()
    return yaml_response({"parcel_id": row[0] if row else None})

@app.post("/get_car_id")
async def get_car_id(request: Request):
    data = await read_yaml(request)
    async with aiosqlite.connect(DB2) as db:
        async with db.execute("SELECT car_id FROM assignments WHERE car_id IS NOT NULL ORDER BY id DESC LIMIT 1") as cur:
            row = await cur.fetchone()
    return yaml_response({"car_id": row[0] if row else None})

@app.post("/update_delivery")
async def update_delivery(request: Request):
    data = await read_yaml(request)
    delivery_id = data.get("delivery_id")
    status = data.get("status")
    info = data.get("info","")
    async with aiosqlite.connect(DB1) as db:
        await db.execute("UPDATE deliveries SET status = ?, info=? WHERE delivery_id = ?", (status, info, delivery_id))
        await db.commit()
    return yaml_response({"status":"ok","updated":delivery_id})

# ui_ms/app.py
from fastapi import FastAPI, Request
from yaml_util import read_yaml, yaml_response
import httpx
import yaml

app = FastAPI(title="UI_MS")

CONTROLLER_URL = "http://server1:8001"
SENDER_CALLBACK = "http://laptop1:8000/ack"  # Sender_MS callback, change for your test

@app.post("/request_delivery")
async def request_delivery(request: Request):
    data = await read_yaml(request)
    # forward to controller
    async with httpx.AsyncClient() as client:
        await client.post(f"{CONTROLLER_URL}/process_request", content=yaml.safe_dump(data), headers={"content-type":"application/x-yaml"})
    # UI acknowledges Controller to caller (later when controller returns, but for simplicity ack)
    return yaml_response({"status":"forwarded_to_controller"})

@app.post("/notify_sender")
async def notify_sender(request: Request):
    data = await read_yaml(request)
    # forward to sender (simulate)
    # use SENDER_CALLBACK (in real world, UI would push to Sender's endpoint)
    async with httpx.AsyncClient() as client:
        await client.post(SENDER_CALLBACK, content=yaml.safe_dump(data), headers={"content-type":"application/x-yaml"})
    return yaml_response({"status":"notified_sender"})

@app.post("/notify_update")
async def notify_update(request: Request):
    data = await read_yaml(request)
    # forward update to sender
    async with httpx.AsyncClient() as client:
        await client.post(SENDER_CALLBACK, content=yaml.safe_dump(data), headers={"content-type":"application/x-yaml"})
    return yaml_response({"status":"update_forwarded"})

# yaml_util.py
import yaml
from fastapi import Request, Response

async def read_yaml(request: Request):
    text = await request.body()
    if not text:
        return {}
    return yaml.safe_load(text)

def yaml_response(data, status_code=200):
    return Response(content=yaml.safe_dump(data), media_type="application/x-yaml", status_code=status_code)

