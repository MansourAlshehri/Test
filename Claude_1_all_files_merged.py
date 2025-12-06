# ==================== COMMON BASE (base_microservice.py) ====================
import yaml
import socket
import threading
import json
from datetime import datetime
from typing import Dict, Any, Callable
import logging

class MicroserviceBase:
    def __init__(self, name: str, host: str, port: int):
        self.name = name
        self.host = host
        self.port = port
        self.handlers: Dict[str, Callable] = {}
        self.logger = logging.getLogger(name)
        logging.basicConfig(level=logging.INFO, 
                          format=f'[{name}] %(asctime)s - %(message)s')
        
    def register_handler(self, action: str, handler: Callable):
        """Register a handler for a specific action"""
        self.handlers[action] = handler
    
    def send_message(self, target_host: str, target_port: int, message: Dict[str, Any]) -> Dict[str, Any]:
        """Send YAML message to another microservice"""
        try:
            yaml_data = yaml.dump(message)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((target_host, target_port))
                s.sendall(yaml_data.encode())
                response = s.recv(4096).decode()
                return yaml.safe_load(response)
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
            return {"status": "error", "message": str(e)}
    
    def handle_request(self, conn, addr):
        """Handle incoming request"""
        try:
            data = conn.recv(4096).decode()
            message = yaml.safe_load(data)
            self.logger.info(f"Received: {message}")
            
            action = message.get('action')
            if action in self.handlers:
                response = self.handlers[action](message)
            else:
                response = {"status": "error", "message": f"Unknown action: {action}"}
            
            yaml_response = yaml.dump(response)
            conn.sendall(yaml_response.encode())
        except Exception as e:
            self.logger.error(f"Error handling request: {e}")
            error_response = yaml.dump({"status": "error", "message": str(e)})
            conn.sendall(error_response.encode())
        finally:
            conn.close()
    
    def start(self):
        """Start the microservice"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        self.logger.info(f"Started on {self.host}:{self.port}")
        
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=self.handle_request, args=(conn, addr))
            thread.start()
# ==================== CAR_MS (car_ms.py) ====================
# External microservice on Windows/Laptop_1
from base_microservice import MicroserviceBase
import random

class CarMS(MicroserviceBase):
    def __init__(self):
        super().__init__("Car_MS", "localhost", 5008)
        self.controller_host = "localhost"
        self.controller_port = 5003
        self.storage_host = "localhost"
        self.storage_port = 5006
        self.current_parcel = None
        
        self.register_handler("request_car_id", self.handle_request_car_id)
        self.register_handler("notify_assignment", self.handle_notify_assignment)
    
    def handle_request_car_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Check and provide car ID"""
        car_id = f"CAR-{random.randint(1000, 9999)}"
        self.logger.info(f"Providing car ID: {car_id}")
        
        # Share car ID with Storage_MS
        storage_message = {
            "action": "store_car_id",
            "source": self.name,
            "car_id": car_id
        }
        storage_response = self.send_message(self.storage_host, self.storage_port, storage_message)
        
        if storage_response.get("status") == "success":
            return {"status": "success", "car_id": car_id}
        else:
            return {"status": "error", "message": "Failed to store car ID"}
    
    def handle_notify_assignment(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery assignment notification"""
        self.current_parcel = message.get("parcel_id")
        self.logger.info(f"Assigned parcel: {self.current_parcel}")
        return {"status": "acknowledged", "source": self.name}
    
    def request_delivery_update(self, status: str):
        """Request delivery update"""
        if not self.current_parcel:
            self.logger.warning("No current parcel assigned")
            return
        
        update_message = {
            "action": "request_delivery_update",
            "source": self.name,
            "parcel_id": self.current_parcel,
            "status": status
        }
        response = self.send_message(self.controller_host, self.controller_port, update_message)
        self.logger.info(f"Delivery update response: {response}")



# ==================== CONTROLLER_MS (controller_ms.py) ====================
# Internal microservice on Ubuntu/Server_1
from base_microservice import MicroserviceBase

class ControllerMS(MicroserviceBase):
    def __init__(self):
        super().__init__("Controller_MS", "localhost", 5003)
        self.idgen_host = "localhost"
        self.idgen_port = 5004
        self.log_host = "localhost"
        self.log_port = 5007
        self.car_host = "localhost"
        self.car_port = 5008
        self.storage_host = "localhost"
        self.storage_port = 5006
        self.ui_host = "localhost"
        self.ui_port = 5002
        
        self.register_handler("process_delivery", self.handle_process_delivery)
        self.register_handler("request_delivery_update", self.handle_delivery_update)
        self.register_handler("acknowledge_notification", self.handle_acknowledge)
    
    def share_log(self, action: str, details: str):
        """Share logs with Log_MS"""
        log_message = {
            "action": "store_log",
            "source": self.name,
            "log_action": action,
            "details": details
        }
        self.send_message(self.log_host, self.log_port, log_message)
    
    def handle_process_delivery(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Main delivery processing workflow"""
        self.logger.info("Processing delivery request")
        
        # Step 1: Request parcel ID from IDGen_MS
        id_message = {"action": "generate_parcel_id", "source": self.name}
        id_response = self.send_message(self.idgen_host, self.idgen_port, id_message)
        
        if id_response.get("status") != "success":
            return {"status": "error", "message": "Failed to generate parcel ID"}
        
        self.share_log("parcel_id_generated", f"Parcel ID: {id_response['parcel_id']}")
        
        # Step 2: Request car ID from Car_MS
        car_message = {"action": "request_car_id", "source": self.name}
        car_response = self.send_message(self.car_host, self.car_port, car_message)
        
        if car_response.get("status") != "success":
            return {"status": "error", "message": "Failed to get car ID"}
        
        self.share_log("car_assigned", f"Car ID: {car_response['car_id']}")
        
        # Step 3: Get parcel ID from Storage_MS
        get_parcel_msg = {"action": "get_parcel_id", "source": self.name}
        parcel_response = self.send_message(self.storage_host, self.storage_port, get_parcel_msg)
        
        # Step 4: Get car ID from Storage_MS
        get_car_msg = {"action": "get_car_id", "source": self.name}
        car_data = self.send_message(self.storage_host, self.storage_port, get_car_msg)
        
        # Step 5: Assign delivery and store in Database_1
        delivery_data = {
            "action": "store_delivery",
            "source": self.name,
            "parcel_id": parcel_response.get("parcel_id"),
            "car_id": car_data.get("car_id")
        }
        delivery_response = self.send_message(self.storage_host, self.storage_port, delivery_data)
        
        self.share_log("delivery_assigned", f"Parcel: {parcel_response.get('parcel_id')}, Car: {car_data.get('car_id')}")
        
        # Step 6: Notify Car_MS
        notify_car = {
            "action": "notify_assignment",
            "source": self.name,
            "parcel_id": parcel_response.get("parcel_id")
        }
        car_ack = self.send_message(self.car_host, self.car_port, notify_car)
        
        self.share_log("car_notified", f"Car acknowledged: {car_ack}")
        
        # Step 7: Notify UI_MS
        notify_ui = {
            "action": "notify_sender",
            "source": self.name,
            "notification_type": "notify_delivery",
            "data": {
                "parcel_id": parcel_response.get("parcel_id"),
                "car_id": car_data.get("car_id"),
                "status": "assigned"
            }
        }
        self.send_message(self.ui_host, self.ui_port, notify_ui)
        
        return {"status": "success", "message": "Delivery processed successfully"}
    
    def handle_delivery_update(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery update from Car_MS"""
        self.logger.info("Processing delivery update")
        
        # Update delivery in Storage_MS
        update_msg = {
            "action": "update_delivery",
            "source": self.name,
            "parcel_id": message.get("parcel_id"),
            "status": message.get("status")
        }
        self.send_message(self.storage_host, self.storage_port, update_msg)
        
        # Notify UI_MS
        notify_ui = {
            "action": "notify_sender",
            "source": self.name,
            "notification_type": "notify_update",
            "data": message
        }
        self.send_message(self.ui_host, self.ui_port, notify_ui)
        
        return {"status": "acknowledged"}
    
    def handle_acknowledge(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle acknowledgments"""
        self.share_log("acknowledgment_received", str(message))
        return {"status": "received"}
# ==================== DATABASE MODELS (database.py) ====================
import sqlite3
from datetime import datetime

class Database:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
    
    def execute(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)
        self.conn.commit()
        return self.cursor

class Database1:
    """Database for parcel data"""
    def __init__(self):
        self.db = Database("database_1.db")
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS parcels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parcel_id TEXT UNIQUE,
                car_id TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
    
    def store_delivery(self, parcel_id: str, car_id: str):
        now = datetime.now().isoformat()
        self.db.execute('''
            INSERT INTO parcels (parcel_id, car_id, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (parcel_id, car_id, 'assigned', now, now))
    
    def update_delivery(self, parcel_id: str, status: str):
        now = datetime.now().isoformat()
        self.db.execute('''
            UPDATE parcels SET status = ?, updated_at = ? WHERE parcel_id = ?
        ''', (status, now, parcel_id))

class Database2:
    """Database for delivery assignments"""
    def __init__(self):
        self.db = Database("database_2.db")
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parcel_id TEXT,
                car_id TEXT,
                created_at TEXT
            )
        ''')
    
    def store_parcel_id(self, parcel_id: str):
        now = datetime.now().isoformat()
        self.db.execute('''
            INSERT INTO assignments (parcel_id, created_at)
            VALUES (?, ?)
        ''', (parcel_id, now))
    
    def store_car_id(self, car_id: str):
        now = datetime.now().isoformat()
        self.db.execute('''
            UPDATE assignments SET car_id = ? 
            WHERE id = (SELECT MAX(id) FROM assignments)
        ''', (car_id,))
    
    def get_parcel_id(self):
        result = self.db.execute('SELECT parcel_id FROM assignments ORDER BY id DESC LIMIT 1')
        row = result.fetchone()
        return row[0] if row else None
    
    def get_car_id(self):
        result = self.db.execute('SELECT car_id FROM assignments ORDER BY id DESC LIMIT 1')
        row = result.fetchone()
        return row[0] if row else None

class Database3:
    """Database for logs"""
    def __init__(self):
        self.db = Database("database_3.db")
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                source TEXT,
                action TEXT,
                details TEXT
            )
        ''')
    
    def store_log(self, source: str, action: str, details: str):
        now = datetime.now().isoformat()
        self.db.execute('''
            INSERT INTO logs (timestamp, source, action, details)
            VALUES (?, ?, ?, ?)
        ''', (now, source, action, details))

# ==================== IDGEN_MS (idgen_ms.py) ====================
# Internal microservice on Ubuntu/Server_1
from base_microservice import MicroserviceBase
import uuid

class IDGenMS(MicroserviceBase):
    def __init__(self):
        super().__init__("IDGen_MS", "localhost", 5004)
        self.storage_host = "localhost"
        self.storage_port = 5006
        
        self.register_handler("generate_parcel_id", self.handle_generate_id)
    
    def handle_generate_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Generate parcel ID and share with Storage_MS"""
        parcel_id = f"PARCEL-{uuid.uuid4().hex[:8].upper()}"
        self.logger.info(f"Generated parcel ID: {parcel_id}")
        
        # Share with Storage_MS
        storage_message = {
            "action": "store_parcel_id",
            "source": self.name,
            "parcel_id": parcel_id
        }
        storage_response = self.send_message(self.storage_host, self.storage_port, storage_message)
        
        if storage_response.get("status") == "success":
            return {
                "status": "success",
                "parcel_id": parcel_id,
                "message": "Parcel ID generated and stored"
            }
        else:
            return {"status": "error", "message": "Failed to store parcel ID"}



# ==================== LOG_MS (log_ms.py) ====================
# Internal microservice on Ubuntu/Server_1
from base_microservice import MicroserviceBase
from database import Database3

class LogMS(MicroserviceBase):
    def __init__(self):
        super().__init__("Log_MS", "localhost", 5007)
        self.db3 = Database3()
        
        self.register_handler("store_log", self.handle_store_log)
    
    def handle_store_log(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Store log in Database_3"""
        source = message.get("source")
        action = message.get("log_action")
        details = message.get("details")
        
        self.db3.store_log(source, action, details)
        self.logger.info(f"Stored log: {source} - {action}")
        return {"status": "success", "message": "Log stored"}
# ==================== MAIN RUNNER (main.py) ====================
import threading
import time

def run_microservice(ms_class):
    """Run a microservice in a separate thread"""
    ms = ms_class()
    ms.start()

if __name__ == "__main__":
    # Start all microservices in separate threads
    microservices = [
        LogMS,
        StorageMS,
        IDGenMS,
        CarMS,
        ControllerMS,
        UIMS,
        SenderMS
    ]
    
    threads = []
    for ms_class in microservices:
        thread = threading.Thread(target=run_microservice, args=(ms_class,), daemon=True)
        thread.start()
        threads.append(thread)
        time.sleep(0.5)  # Stagger startup
    
    # Wait for all services to start
    time.sleep(2)
    
    print("\n" + "="*60)
    print("ALL MICROSERVICES STARTED")
    print("="*60 + "\n")
    
    # Simulate a delivery request
    print("Simulating delivery request...")
    time.sleep(1)
    
    sender = SenderMS()
    sender_info = {
        "sender_name": "John Doe",
        "pickup_address": "123 Main St",
        "delivery_address": "456 Oak Ave"
    }
    
    # Start sender in thread (it has its own server)
    sender_thread = threading.Thread(target=sender.start, daemon=True)
    sender_thread.start()
    time.sleep(1)
    
    # Make delivery request
    sender.request_delivery(sender_info)
    
    # Simulate car update after 5 seconds
    time.sleep(5)
    print("\nSimulating delivery status update...")
    car = CarMS()
    car_thread = threading.Thread(target=car.start, daemon=True)
    car_thread.start()
    time.sleep(1)
    car.request_delivery_update("in_transit")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
# ==================== SENDER_MS (sender_ms.py) ====================
# External microservice on Windows/Laptop_1
from base_microservice import MicroserviceBase
import time

class SenderMS(MicroserviceBase):
    def __init__(self):
        super().__init__("Sender_MS", "localhost", 5001)
        self.ui_host = "localhost"  # Would be Server_1 IP in production
        self.ui_port = 5002
        
        self.register_handler("notify_delivery", self.handle_notify)
        self.register_handler("notify_update", self.handle_update_notification)
    
    def request_delivery(self, sender_info: Dict[str, Any]):
        """Request delivery from UI_MS"""
        message = {
            "action": "request_delivery",
            "source": self.name,
            "sender_info": sender_info,
            "timestamp": datetime.now().isoformat()
        }
        response = self.send_message(self.ui_host, self.ui_port, message)
        self.logger.info(f"Delivery request response: {response}")
        
        # Acknowledge UI_MS
        ack_message = {
            "action": "acknowledge",
            "source": self.name,
            "status": "received"
        }
        self.send_message(self.ui_host, self.ui_port, ack_message)
        return response
    
    def handle_notify(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery notification from UI_MS"""
        self.logger.info(f"Delivery notification: {message}")
        return {"status": "acknowledged", "source": self.name}
    
    def handle_update_notification(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery update notification"""
        self.logger.info(f"Delivery update: {message}")
        return {"status": "acknowledged", "source": self.name}
# ==================== STORAGE_MS (storage_ms.py) ====================
# Internal microservice on Ubuntu/Server_1
from base_microservice import MicroserviceBase
from database import Database1, Database2

class StorageMS(MicroserviceBase):
    def __init__(self):
        super().__init__("Storage_MS", "localhost", 5006)
        self.db1 = Database1()
        self.db2 = Database2()
        
        self.register_handler("store_parcel_id", self.handle_store_parcel_id)
        self.register_handler("store_car_id", self.handle_store_car_id)
        self.register_handler("get_parcel_id", self.handle_get_parcel_id)
        self.register_handler("get_car_id", self.handle_get_car_id)
        self.register_handler("store_delivery", self.handle_store_delivery)
        self.register_handler("update_delivery", self.handle_update_delivery)
    
    def handle_store_parcel_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Store parcel ID in Database_2"""
        parcel_id = message.get("parcel_id")
        self.db2.store_parcel_id(parcel_id)
        self.logger.info(f"Stored parcel ID: {parcel_id}")
        return {"status": "success", "message": "Parcel ID stored"}
    
    def handle_store_car_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Store car ID in Database_2"""
        car_id = message.get("car_id")
        self.db2.store_car_id(car_id)
        self.logger.info(f"Stored car ID: {car_id}")
        return {"status": "success", "message": "Car ID stored"}
    
    def handle_get_parcel_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Get parcel ID from Database_2"""
        parcel_id = self.db2.get_parcel_id()
        return {"status": "success", "parcel_id": parcel_id}
    
    def handle_get_car_id(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Get car ID from Database_2"""
        car_id = self.db2.get_car_id()
        return {"status": "success", "car_id": car_id}
    
    def handle_store_delivery(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Store delivery in Database_1"""
        parcel_id = message.get("parcel_id")
        car_id = message.get("car_id")
        self.db1.store_delivery(parcel_id, car_id)
        self.logger.info(f"Stored delivery: {parcel_id} -> {car_id}")
        return {"status": "success", "message": "Delivery stored"}
    
    def handle_update_delivery(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Update delivery in Database_1"""
        parcel_id = message.get("parcel_id")
        status = message.get("status")
        self.db1.update_delivery(parcel_id, status)
        self.logger.info(f"Updated delivery: {parcel_id} -> {status}")
        return {"status": "success", "message": "Delivery updated"}
# ==================== UI_MS (ui_ms.py) ====================
# Internal microservice on Ubuntu/Server_1
from base_microservice import MicroserviceBase

class UIMS(MicroserviceBase):
    def __init__(self):
        super().__init__("UI_MS", "localhost", 5002)
        self.controller_host = "localhost"
        self.controller_port = 5003
        
        self.register_handler("request_delivery", self.handle_request_delivery)
        self.register_handler("notify_sender", self.handle_notify_sender)
        self.register_handler("acknowledge", self.handle_acknowledge)
    
    def handle_request_delivery(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Forward delivery request to Controller_MS"""
        self.logger.info("Forwarding delivery request to Controller_MS")
        
        forward_message = {
            "action": "process_delivery",
            "source": self.name,
            "original_request": message
        }
        response = self.send_message(self.controller_host, self.controller_port, forward_message)
        return response
    
    def handle_notify_sender(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Notify Sender_MS"""
        sender_host = "localhost"  # Would be Laptop_1 IP in production
        sender_port = 5001
        
        notify_message = {
            "action": message.get("notification_type", "notify_delivery"),
            "source": self.name,
            "data": message.get("data")
        }
        response = self.send_message(sender_host, sender_port, notify_message)
        
        # Acknowledge Controller_MS
        ack_message = {
            "action": "acknowledge_notification",
            "source": self.name,
            "status": "sender_notified"
        }
        self.send_message(self.controller_host, self.controller_port, ack_message)
        
        return {"status": "success"}
    
    def handle_acknowledge(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle acknowledgment from Sender_MS"""
        return {"status": "received"}
