# ============================================================================
# SHARED UTILITIES AND BASE CLASSES TEST
# ============================================================================

import yaml
import socket
import threading
import sqlite3
import uuid
import time
from datetime import datetime
from typing import Dict, Any, Optional
import json

class YAMLMessage:
    """Handles YAML message serialization/deserialization"""
    
    @staticmethod
    def serialize(data: Dict[str, Any]) -> bytes:
        """Convert dictionary to YAML bytes"""
        yaml_str = yaml.dump(data, default_flow_style=False)
        return yaml_str.encode('utf-8')
    
    @staticmethod
    def deserialize(data: bytes) -> Dict[str, Any]:
        """Convert YAML bytes to dictionary"""
        yaml_str = data.decode('utf-8')
        return yaml.safe_load(yaml_str)

class MicroserviceBase:
    """Base class for all microservices"""
    
    def __init__(self, name: str, host: str, port: int):
        self.name = name
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        
    def start(self):
        """Start the microservice server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        print(f"[{self.name}] Started on {self.host}:{self.port}")
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                thread.start()
            except Exception as e:
                if self.running:
                    print(f"[{self.name}] Error: {e}")
    
    def handle_client(self, client_socket: socket.socket):
        """Handle incoming client connections"""
        try:
            data = client_socket.recv(4096)
            if data:
                message = YAMLMessage.deserialize(data)
                response = self.process_message(message)
                if response:
                    client_socket.send(YAMLMessage.serialize(response))
        except Exception as e:
            print(f"[{self.name}] Error handling client: {e}")
        finally:
            client_socket.close()
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Override this method to process messages"""
        raise NotImplementedError
    
    def send_message(self, host: str, port: int, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send message to another microservice"""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((host, port))
            client_socket.send(YAMLMessage.serialize(message))
            
            response_data = client_socket.recv(4096)
            client_socket.close()
            
            if response_data:
                return YAMLMessage.deserialize(response_data)
            return None
        except Exception as e:
            print(f"[{self.name}] Error sending message to {host}:{port} - {e}")
            return None
    
    def stop(self):
        """Stop the microservice"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


# ============================================================================
# SENDER_MS (External - Windows/Laptop_1)
# ============================================================================

class SenderMS(MicroserviceBase):
    """External microservice for requesting deliveries"""
    
    def __init__(self):
        super().__init__("Sender_MS", "0.0.0.0", 5001)
        self.ui_ms_host = "192.168.1.100"  # Server_1 IP
        self.ui_ms_port = 5002
    
    def request_delivery(self, sender_name: str, recipient_name: str, 
                        pickup_address: str, delivery_address: str, parcel_weight: float):
        """Request a delivery through UI_MS"""
        message = {
            'action': 'request_delivery',
            'sender_name': sender_name,
            'recipient_name': recipient_name,
            'pickup_address': pickup_address,
            'delivery_address': delivery_address,
            'parcel_weight': parcel_weight,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"[{self.name}] Requesting delivery...")
        response = self.send_message(self.ui_ms_host, self.ui_ms_port, message)
        
        if response and response.get('status') == 'success':
            print(f"[{self.name}] Delivery request accepted: {response.get('message')}")
            return response
        else:
            print(f"[{self.name}] Delivery request failed")
            return None
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle incoming notifications"""
        action = message.get('action')
        
        if action == 'delivery_assigned':
            parcel_id = message.get('parcel_id')
            car_id = message.get('car_id')
            print(f"[{self.name}] Delivery assigned - Parcel: {parcel_id}, Car: {car_id}")
            return {'status': 'acknowledged'}
        
        elif action == 'delivery_update':
            status = message.get('delivery_status')
            print(f"[{self.name}] Delivery update received: {status}")
            return {'status': 'acknowledged'}
        
        return {'status': 'unknown_action'}


# ============================================================================
# UI_MS (Internal - Ubuntu/Server_1)
# ============================================================================

class UI_MS(MicroserviceBase):
    """User Interface microservice"""
    
    def __init__(self):
        super().__init__("UI_MS", "0.0.0.0", 5002)
        self.controller_ms_host = "localhost"
        self.controller_ms_port = 5003
        self.sender_ms_host = None  # Will be set from incoming requests
        self.sender_ms_port = 5001
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process incoming messages"""
        action = message.get('action')
        
        if action == 'request_delivery':
            # Store sender info for later notifications
            print(f"[{self.name}] Received delivery request")
            
            # Forward to Controller_MS
            response = self.send_message(self.controller_ms_host, self.controller_ms_port, message)
            return response
        
        elif action == 'notify_delivery_assigned':
            # Notify Sender_MS
            parcel_id = message.get('parcel_id')
            car_id = message.get('car_id')
            print(f"[{self.name}] Notifying sender about delivery assignment")
            
            notification = {
                'action': 'delivery_assigned',
                'parcel_id': parcel_id,
                'car_id': car_id
            }
            
            # In real implementation, track sender connection info
            # For demo, assume sender_ms_host is known
            if self.sender_ms_host:
                sender_response = self.send_message(self.sender_ms_host, self.sender_ms_port, notification)
                
            return {'status': 'acknowledged'}
        
        elif action == 'notify_delivery_update':
            # Notify Sender_MS about delivery update
            print(f"[{self.name}] Notifying sender about delivery update")
            
            notification = {
                'action': 'delivery_update',
                'delivery_status': message.get('delivery_status')
            }
            
            if self.sender_ms_host:
                sender_response = self.send_message(self.sender_ms_host, self.sender_ms_port, notification)
                
            return {'status': 'acknowledged'}
        
        return {'status': 'unknown_action'}


# ============================================================================
# IDGEN_MS (Internal - Ubuntu/Server_1)
# ============================================================================

class IDGen_MS(MicroserviceBase):
    """ID Generation microservice"""
    
    def __init__(self):
        super().__init__("IDGen_MS", "0.0.0.0", 5004)
        self.storage_ms_host = "localhost"
        self.storage_ms_port = 5005
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate IDs on request"""
        action = message.get('action')
        
        if action == 'generate_parcel_id':
            # Generate unique parcel ID
            parcel_id = f"PARCEL-{uuid.uuid4().hex[:8].upper()}"
            print(f"[{self.name}] Generated parcel ID: {parcel_id}")
            
            # Share with Storage_MS
            storage_message = {
                'action': 'store_parcel_id',
                'parcel_id': parcel_id,
                'parcel_data': message.get('parcel_data')
            }
            
            storage_response = self.send_message(self.storage_ms_host, self.storage_ms_port, storage_message)
            
            if storage_response and storage_response.get('status') == 'success':
                print(f"[{self.name}] Parcel ID stored successfully")
                return {
                    'status': 'success',
                    'parcel_id': parcel_id
                }
            else:
                return {'status': 'error', 'message': 'Failed to store parcel ID'}
        
        return {'status': 'unknown_action'}


# ============================================================================
# CONTROLLER_MS (Internal - Ubuntu/Server_1)
# ============================================================================

class Controller_MS(MicroserviceBase):
    """Main controller microservice"""
    
    def __init__(self):
        super().__init__("Controller_MS", "0.0.0.0", 5003)
        self.idgen_ms_host = "localhost"
        self.idgen_ms_port = 5004
        self.storage_ms_host = "localhost"
        self.storage_ms_port = 5005
        self.car_ms_host = "192.168.1.101"  # Laptop_1 IP
        self.car_ms_port = 5006
        self.log_ms_host = "localhost"
        self.log_ms_port = 5007
        self.ui_ms_host = "localhost"
        self.ui_ms_port = 5002
    
    def log_event(self, event: str, data: Dict[str, Any]):
        """Send log to Log_MS"""
        log_message = {
            'action': 'store_log',
            'event': event,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        self.send_message(self.log_ms_host, self.log_ms_port, log_message)
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Main delivery workflow controller"""
        action = message.get('action')
        
        if action == 'request_delivery':
            return self.handle_delivery_request(message)
        
        elif action == 'request_delivery_update':
            return self.handle_delivery_update(message)
        
        return {'status': 'unknown_action'}
    
    def handle_delivery_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery request workflow"""
        print(f"[{self.name}] Processing delivery request...")
        
        # Step 1: Request parcel ID from IDGen_MS
        idgen_request = {
            'action': 'generate_parcel_id',
            'parcel_data': message
        }
        idgen_response = self.send_message(self.idgen_ms_host, self.idgen_ms_port, idgen_request)
        
        if not idgen_response or idgen_response.get('status') != 'success':
            self.log_event('delivery_request_failed', {'reason': 'ID generation failed'})
            return {'status': 'error', 'message': 'Failed to generate parcel ID'}
        
        parcel_id = idgen_response.get('parcel_id')
        self.log_event('parcel_id_generated', {'parcel_id': parcel_id})
        
        # Step 2: Request car ID from Car_MS
        car_request = {
            'action': 'request_car_id',
            'parcel_id': parcel_id
        }
        car_response = self.send_message(self.car_ms_host, self.car_ms_port, car_request)
        
        if not car_response or car_response.get('status') != 'success':
            self.log_event('car_assignment_failed', {'parcel_id': parcel_id})
            return {'status': 'error', 'message': 'Failed to assign car'}
        
        car_id = car_response.get('car_id')
        self.log_event('car_assigned', {'parcel_id': parcel_id, 'car_id': car_id})
        
        # Step 3: Get parcel ID and car ID from Storage_MS (verification)
        storage_parcel_req = {
            'action': 'get_parcel_id',
            'parcel_id': parcel_id
        }
        storage_parcel_resp = self.send_message(self.storage_ms_host, self.storage_ms_port, storage_parcel_req)
        
        storage_car_req = {
            'action': 'get_car_id',
            'car_id': car_id
        }
        storage_car_resp = self.send_message(self.storage_ms_host, self.storage_ms_port, storage_car_req)
        
        # Step 4: Assign delivery
        delivery_data = {
            'parcel_id': parcel_id,
            'car_id': car_id,
            'status': 'assigned',
            'delivery_details': message
        }
        
        storage_delivery_req = {
            'action': 'store_delivery',
            'delivery_data': delivery_data
        }
        storage_delivery_resp = self.send_message(self.storage_ms_host, self.storage_ms_port, storage_delivery_req)
        
        if not storage_delivery_resp or storage_delivery_resp.get('status') != 'success':
            self.log_event('delivery_storage_failed', {'parcel_id': parcel_id})
            return {'status': 'error', 'message': 'Failed to store delivery'}
        
        self.log_event('delivery_assigned', {'parcel_id': parcel_id, 'car_id': car_id})
        
        # Step 5: Notify Car_MS
        car_notification = {
            'action': 'notify_delivery_assignment',
            'parcel_id': parcel_id,
            'car_id': car_id,
            'delivery_details': message
        }
        car_notif_resp = self.send_message(self.car_ms_host, self.car_ms_port, car_notification)
        self.log_event('car_notified', {'parcel_id': parcel_id, 'car_id': car_id})
        
        # Step 6: Notify UI_MS
        ui_notification = {
            'action': 'notify_delivery_assigned',
            'parcel_id': parcel_id,
            'car_id': car_id
        }
        ui_resp = self.send_message(self.ui_ms_host, self.ui_ms_port, ui_notification)
        self.log_event('ui_notified', {'parcel_id': parcel_id})
        
        return {
            'status': 'success',
            'message': 'Delivery assigned successfully',
            'parcel_id': parcel_id,
            'car_id': car_id
        }
    
    def handle_delivery_update(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle delivery update from Car_MS"""
        print(f"[{self.name}] Processing delivery update...")
        
        parcel_id = message.get('parcel_id')
        new_status = message.get('status')
        
        # Update delivery in Storage_MS
        update_request = {
            'action': 'update_delivery',
            'parcel_id': parcel_id,
            'status': new_status
        }
        storage_resp = self.send_message(self.storage_ms_host, self.storage_ms_port, update_request)
        
        if storage_resp and storage_resp.get('status') == 'success':
            # Notify UI_MS
            ui_notification = {
                'action': 'notify_delivery_update',
                'parcel_id': parcel_id,
                'delivery_status': new_status
            }
            self.send_message(self.ui_ms_host, self.ui_ms_port, ui_notification)
            
            self.log_event('delivery_updated', {'parcel_id': parcel_id, 'status': new_status})
            
            return {'status': 'success', 'message': 'Delivery updated'}
        
        return {'status': 'error', 'message': 'Failed to update delivery'}


# ============================================================================
# STORAGE_MS (Internal - Ubuntu/Server_1)
# ============================================================================

class Storage_MS(MicroserviceBase):
    """Storage microservice for database operations"""
    
    def __init__(self):
        super().__init__("Storage_MS", "0.0.0.0", 5005)
        self.init_databases()
    
    def init_databases(self):
        """Initialize SQLite databases"""
        # Database_1: Parcel data
        self.db1_conn = sqlite3.connect('database_1_parcels.db', check_same_thread=False)
        self.db1_cursor = self.db1_conn.cursor()
        self.db1_cursor.execute('''
            CREATE TABLE IF NOT EXISTS parcels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parcel_id TEXT UNIQUE,
                sender_name TEXT,
                recipient_name TEXT,
                pickup_address TEXT,
                delivery_address TEXT,
                parcel_weight REAL,
                status TEXT,
                created_at TEXT
            )
        ''')
        self.db1_conn.commit()
        
        # Database_2: Delivery assignments
        self.db2_conn = sqlite3.connect('database_2_assignments.db', check_same_thread=False)
        self.db2_cursor = self.db2_conn.cursor()
        self.db2_cursor.execute('''
            CREATE TABLE IF NOT EXISTS assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parcel_id TEXT,
                car_id TEXT,
                assigned_at TEXT
            )
        ''')
        self.db2_conn.commit()
        
        print(f"[{self.name}] Databases initialized")
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle storage operations"""
        action = message.get('action')
        
        if action == 'store_parcel_id':
            parcel_id = message.get('parcel_id')
            parcel_data = message.get('parcel_data', {})
            
            try:
                self.db2_cursor.execute(
                    'INSERT INTO assignments (parcel_id, assigned_at) VALUES (?, ?)',
                    (parcel_id, datetime.now().isoformat())
                )
                self.db2_conn.commit()
                print(f"[{self.name}] Stored parcel ID: {parcel_id}")
                return {'status': 'success'}
            except Exception as e:
                print(f"[{self.name}] Error storing parcel ID: {e}")
                return {'status': 'error', 'message': str(e)}
        
        elif action == 'store_car_id':
            parcel_id = message.get('parcel_id')
            car_id = message.get('car_id')
            
            try:
                self.db2_cursor.execute(
                    'UPDATE assignments SET car_id = ? WHERE parcel_id = ?',
                    (car_id, parcel_id)
                )
                self.db2_conn.commit()
                print(f"[{self.name}] Stored car ID: {car_id} for parcel: {parcel_id}")
                return {'status': 'success'}
            except Exception as e:
                print(f"[{self.name}] Error storing car ID: {e}")
                return {'status': 'error', 'message': str(e)}
        
        elif action == 'store_delivery':
            delivery_data = message.get('delivery_data', {})
            parcel_id = delivery_data.get('parcel_id')
            car_id = delivery_data.get('car_id')
            details = delivery_data.get('delivery_details', {})
            
            try:
                self.db1_cursor.execute('''
                    INSERT INTO parcels (parcel_id, sender_name, recipient_name, 
                                       pickup_address, delivery_address, parcel_weight, 
                                       status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    parcel_id,
                    details.get('sender_name'),
                    details.get('recipient_name'),
                    details.get('pickup_address'),
                    details.get('delivery_address'),
                    details.get('parcel_weight'),
                    'assigned',
                    datetime.now().isoformat()
                ))
                self.db1_conn.commit()
                print(f"[{self.name}] Stored delivery: {parcel_id}")
                return {'status': 'success'}
            except Exception as e:
                print(f"[{self.name}] Error storing delivery: {e}")
                return {'status': 'error', 'message': str(e)}
        
        elif action == 'update_delivery':
            parcel_id = message.get('parcel_id')
            status = message.get('status')
            
            try:
                self.db1_cursor.execute(
                    'UPDATE parcels SET status = ? WHERE parcel_id = ?',
                    (status, parcel_id)
                )
                self.db1_conn.commit()
                print(f"[{self.name}] Updated delivery {parcel_id} to status: {status}")
                return {'status': 'success'}
            except Exception as e:
                print(f"[{self.name}] Error updating delivery: {e}")
                return {'status': 'error', 'message': str(e)}
        
        elif action == 'get_parcel_id':
            parcel_id = message.get('parcel_id')
            self.db2_cursor.execute('SELECT * FROM assignments WHERE parcel_id = ?', (parcel_id,))
            result = self.db2_cursor.fetchone()
            if result:
                return {'status': 'success', 'parcel_id': parcel_id}
            return {'status': 'not_found'}
        
        elif action == 'get_car_id':
            car_id = message.get('car_id')
            self.db2_cursor.execute('SELECT * FROM assignments WHERE car_id = ?', (car_id,))
            result = self.db2_cursor.fetchone()
            if result:
                return {'status': 'success', 'car_id': car_id}
            return {'status': 'not_found'}
        
        return {'status': 'unknown_action'}


# ============================================================================
# LOG_MS (Internal - Ubuntu/Server_1)
# ============================================================================

class Log_MS(MicroserviceBase):
    """Logging microservice"""
    
    def __init__(self):
        super().__init__("Log_MS", "0.0.0.0", 5007)
        self.init_database()
    
    def init_database(self):
        """Initialize logging database"""
        self.db_conn = sqlite3.connect('database_3_logs.db', check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()
        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT,
                data TEXT,
                timestamp TEXT
            )
        ''')
        self.db_conn.commit()
        print(f"[{self.name}] Log database initialized")
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Store logs"""
        action = message.get('action')
        
        if action == 'store_log':
            event = message.get('event')
            data = json.dumps(message.get('data', {}))
            timestamp = message.get('timestamp')
            
            try:
                self.db_cursor.execute(
                    'INSERT INTO logs (event, data, timestamp) VALUES (?, ?, ?)',
                    (event, data, timestamp)
                )
                self.db_conn.commit()
                print(f"[{self.name}] Logged event: {event}")
                return {'status': 'success'}
            except Exception as e:
                print(f"[{self.name}] Error storing log: {e}")
                return {'status': 'error', 'message': str(e)}
        
        return {'status': 'unknown_action'}


# ============================================================================
# CAR_MS (External - Windows/Laptop_1)
# ============================================================================

class Car_MS(MicroserviceBase):
    """External car microservice"""
    
    def __init__(self):
        super().__init__("Car_MS", "0.0.0.0", 5006)
        self.available_cars = ['CAR-001', 'CAR-002', 'CAR-003']
        self.storage_ms_host = "192.168.1.100"  # Server_1 IP
        self.storage_ms_port = 5005
        self.controller_ms_host = "192.168.1.100"
        self.controller_ms_port = 5003
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle car-related requests"""
        action = message.get('action')
        
        if action == 'request_car_id':
            # Check and assign available car
            if self.available_cars:
                car_id = self.available_cars[0]  # Simple assignment
                parcel_id = message.get('parcel_id')
                
                print(f"[{self.name}] Assigning car: {car_id}")
                
                # Share car ID with Storage_MS
                storage_message = {
                    'action': 'store_car_id',
                    'car_id': car_id,
                    'parcel_id': parcel_id
                }
                
                storage_response = self.send_message(self.storage_ms_host, self.storage_ms_port, storage_message)
                
                if storage_response and storage_response.get('status') == 'success':
                    return {
                        'status': 'success',
                        'car_id': car_id
                    }
                else:
                    return {'status': 'error', 'message': 'Failed to store car assignment'}
            else:
                return {'status': 'error', 'message': 'No cars available'}
        
        elif action == 'notify_delivery_assignment':
            parcel_id = message.get('parcel_id')
            car_id = message.get('car_id')
            print(f"[{self.name}] Delivery assignment received - Parcel: {parcel_id}, Car: {car_id}")
            return {'status': 'acknowledged'}
        
        return {'status': 'unknown_action'}
    
    def request_delivery_update(self, parcel_id: str, new_status: str):
        """Request to update delivery status"""
        message = {
            'action': 'request_delivery_update',
            'parcel_id': parcel_id,
            'status': new_status
        }
        
        print(f"[{self.name}] Requesting delivery update for {parcel_id}: {new_status}")
        response = self.send_message(self.controller_ms_host, self.controller_ms_port, message)
        
        if response and response.get('status') == 'success':
            print(f"[{self.name}] Delivery update acknowledged")
        else:
            print(f"[{self.name}] Delivery update failed")


# ============================================================================
# DEMO / TESTING
# ============================================================================

def run_microservice(ms_class):
    """Helper to run a microservice in a thread"""
    ms = ms_class()
    thread = threading.Thread(target=ms.start)
    thread.daemon = True
    thread.start()
    return ms

if __name__ == "__main__":
    print("=" * 80)
    print("DELIVERY MANAGEMENT MICROSERVICES SYSTEM")
    print("=" * 80)
    
    # Start all internal microservices (Server_1)
    print("\nStarting internal microservices on Server_1...")
    ui_ms = run_microservice(UI_MS)
    controller_ms = run_microservice(Controller_MS)
    idgen_ms = run_microservice(IDGen_MS)
    storage_ms = run_microservice(Storage_MS)
    log_ms = run_microservice(Log_MS)
    
    time.sleep(2)  # Wait for services to start
    
    # Start external microservices (Laptop_1)
    print("\nStarting external microservices on Laptop_1...")
    sender_ms = SenderMS()
    car_ms = run_microservice(Car_MS)
    
    time.sleep(2)
    
    print("\n" + "=" * 80)
    print("ALL MICROSERVICES STARTED")
    print("=" * 80)
    
    # Demo: Request a delivery
    print("\n--- DEMO: Requesting Delivery ---\n")
    time.sleep(1)
    
    sender_ms.request_delivery(
        sender_name="John Doe",
        recipient_name="Jane Smith",
        pickup_address="123 Main St, City A",
        delivery_address="456 Oak Ave, City B",
        parcel_weight=5.5
    )
    
    time.sleep(3)
    
    # Demo: Car updates delivery status
    print("\n--- DEMO: Car Updating Delivery Status ---\n")
    car_ms.request_delivery_update("PARCEL-DEMO", "
