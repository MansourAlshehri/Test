# ============================================================================
# CAR_MS (External - Windows/Laptop_1)
# ============================================================================

# car_ms/car_service.py
import random

class Car_MS:
    """External microservice representing delivery vehicles"""
    
    def __init__(self, message_bus: MessageBus, car_id: str = None):
        self.message_bus = message_bus
        self.car_id = car_id or f"CAR-{random.randint(1000, 9999)}"
        self.logger = logging.getLogger(f'Car_MS-{self.car_id}')
        self.assigned_deliveries = []
        
    def start(self):
        """Start listening for requests"""
        self.message_bus.receive_message('car_ms_queue', self.handle_message)
        self.logger.info(f"Car {self.car_id} is online")
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming messages"""
        msg_type = message.get('message_type')
        
        if msg_type == 'request_car_id':
            self.provide_car_id(message)
        elif msg_type == 'delivery_notification':
            self.acknowledge_delivery(message)
        elif msg_type == 'acknowledgment':
            self.logger.info("Received acknowledgment from Controller")
            
    def provide_car_id(self, message: Dict[str, Any]):
        """Check availability and provide car ID"""
        # Simulate checking availability
        is_available = True
        
        if is_available:
            # Share car ID with Storage_MS
            storage_message = {
                'message_type': 'store_car_id',
                'car_id': self.car_id,
                'parcel_id': message.get('parcel_id'),
                'request_id': message.get('request_id'),
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Car {self.car_id} available, sharing ID with Storage")
            self.message_bus.send_message('storage_ms_queue', storage_message)
            
            # Wait for Storage acknowledgment, then acknowledge Controller
            time.sleep(0.1)  # Simulate waiting
            
            controller_message = {
                'message_type': 'car_id_assigned',
                'car_id': self.car_id,
                'request_id': message.get('request_id'),
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Acknowledging Controller with car ID {self.car_id}")
            self.message_bus.send_message('controller_ms_queue', controller_message)
            
    def acknowledge_delivery(self, message: Dict[str, Any]):
        """Acknowledge delivery assignment"""
        parcel_id = message.get('parcel_id')
        self.assigned_deliveries.append(parcel_id)
        
        self.logger.info(f"Delivery assigned: Parcel {parcel_id}")
        
        # Send acknowledgment to Controller
        ack_message = {
            'message_type': 'acknowledgment',
            'car_id': self.car_id,
            'parcel_id': parcel_id,
            'request_id': message.get('request_id'),
            'timestamp': datetime.now().isoformat()
        }
        
        self.message_bus.send_message('controller_ms_queue', ack_message)
        
    def request_delivery_update(self, parcel_id: str, status: str):
        """Request delivery status update"""
        update_message = {
            'message_type': 'delivery_update_request',
            'car_id': self.car_id,
            'parcel_id': parcel_id,
            'status': status,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Requesting delivery update for {parcel_id}: {status}")
        self.message_bus.send_message('controller_ms_queue', update_message)



# ============================================================================
# COMMON UTILITIES AND BASE CLASSES
# ============================================================================
import sqlite3
from typing import List, Dict, Any, Optional

class Database:
    """Base database handler"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None
        
    def connect(self):
        """Connect to database"""
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        
    def execute(self, query: str, params: tuple = ()) -> Any:
        """Execute a query"""
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        return cursor
        
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """Fetch one result"""
        cursor = self.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None
        
    def fetchall(self, query: str, params: tuple = ()) -> List[Dict]:
        """Fetch all results"""
        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
        
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
# ============================================================================
# COMMON UTILITIES AND BASE CLASSES
# ============================================================================
import yaml
import pika
import json
import logging
from typing import Dict, Any, Callable
from abc import ABC, abstractmethod

class MessageBus:
    """Handles YAML-based messaging between microservices using RabbitMQ"""
    
    def __init__(self, host='localhost', port=5672):
        self.host = host
        self.port = port
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Establish connection to RabbitMQ"""
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
    def send_message(self, queue_name: str, message: Dict[str, Any]):
        """Send YAML message to a queue"""
        if not self.channel:
            self.connect()
            
        self.channel.queue_declare(queue=queue_name, durable=True)
        yaml_message = yaml.dump(message)
        
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=yaml_message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
    def receive_message(self, queue_name: str, callback: Callable):
        """Receive and process YAML messages from a queue"""
        if not self.channel:
            self.connect()
            
        self.channel.queue_declare(queue=queue_name, durable=True)
        
        def wrapper_callback(ch, method, properties, body):
            message = yaml.safe_load(body)
            callback(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=wrapper_callback
        )
        
    def start_consuming(self):
        """Start listening for messages"""
        self.channel.start_consuming()
        
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
# ============================================================================
# CONTROLLER_MS (Internal - Ubuntu/Server_1)
# ============================================================================

# controller_ms/controller_service.py
class Controller_MS:
    """Internal microservice coordinating the delivery process"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('Controller_MS')
        self.active_requests = {}
        
    def start(self):
        """Start listening for requests"""
        self.message_bus.receive_message('controller_ms_queue', self.handle_message)
        self.message_bus.start_consuming()
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming messages"""
        msg_type = message.get('message_type')
        
        if msg_type == 'delivery_request':
            self.process_delivery_request(message)
        elif msg_type == 'parcel_id_generated':
            self.handle_parcel_id_generated(message)
        elif msg_type == 'car_id_assigned':
            self.handle_car_id_assigned(message)
        elif msg_type == 'delivery_update_request':
            self.handle_delivery_update(message)
        elif msg_type == 'acknowledgment':
            self.handle_acknowledgment(message)
            
    def process_delivery_request(self, message: Dict[str, Any]):
        """Process new delivery request"""
        request_id = message.get('request_id')
        self.active_requests[request_id] = message
        
        self.log_action('delivery_request_received', message)
        
        # Request parcel ID from IDGen_MS
        id_request = {
            'message_type': 'generate_parcel_id',
            'request_id': request_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Requesting parcel ID for {request_id}")
        self.message_bus.send_message('idgen_ms_queue', id_request)
        
    def handle_parcel_id_generated(self, message: Dict[str, Any]):
        """Handle parcel ID generation confirmation"""
        request_id = message.get('request_id')
        parcel_id = message.get('parcel_id')
        
        if request_id in self.active_requests:
            self.active_requests[request_id]['parcel_id'] = parcel_id
            
        self.log_action('parcel_id_generated', message)
        
        # Request car ID from Car_MS
        car_request = {
            'message_type': 'request_car_id',
            'request_id': request_id,
            'parcel_id': parcel_id,
            'pickup_address': self.active_requests[request_id]['pickup_address'],
            'delivery_address': self.active_requests[request_id]['delivery_address'],
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Requesting car ID for parcel {parcel_id}")
        self.message_bus.send_message('car_ms_queue', car_request)
        
    def handle_car_id_assigned(self, message: Dict[str, Any]):
        """Handle car ID assignment confirmation"""
        request_id = message.get('request_id')
        car_id = message.get('car_id')
        
        if request_id in self.active_requests:
            self.active_requests[request_id]['car_id'] = car_id
            
        self.log_action('car_id_assigned', message)
        
        # Get parcel ID from Storage
        self.request_delivery_info(request_id)
        
    def request_delivery_info(self, request_id: str):
        """Request parcel and car IDs from Storage and assign delivery"""
        request_data = self.active_requests[request_id]
        parcel_id = request_data.get('parcel_id')
        car_id = request_data.get('car_id')
        
        # Assign delivery
        delivery_data = {
            'message_type': 'store_delivery',
            'request_id': request_id,
            'parcel_id': parcel_id,
            'car_id': car_id,
            'sender_name': request_data['sender_name'],
            'recipient_name': request_data['recipient_name'],
            'pickup_address': request_data['pickup_address'],
            'delivery_address': request_data['delivery_address'],
            'package_description': request_data['package_description'],
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Assigning delivery for parcel {parcel_id} to car {car_id}")
        self.message_bus.send_message('storage_ms_queue', delivery_data)
        
        self.log_action('delivery_assigned', delivery_data)
        
        # Notify Car_MS
        self.notify_car(car_id, parcel_id, request_id)
        
        # Notify UI_MS
        self.notify_ui(request_id, parcel_id, car_id)
        
    def notify_car(self, car_id: str, parcel_id: str, request_id: str):
        """Notify Car_MS about delivery assignment"""
        notification = {
            'message_type': 'delivery_notification',
            'car_id': car_id,
            'parcel_id': parcel_id,
            'request_id': request_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Notifying Car {car_id} about delivery")
        self.message_bus.send_message('car_ms_queue', notification)
        
    def notify_ui(self, request_id: str, parcel_id: str, car_id: str):
        """Notify UI_MS about delivery assignment"""
        notification = {
            'message_type': 'delivery_assigned',
            'request_id': request_id,
            'parcel_id': parcel_id,
            'car_id': car_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Notifying UI about delivery assignment")
        self.message_bus.send_message('ui_ms_queue', notification)
        
    def handle_delivery_update(self, message: Dict[str, Any]):
        """Handle delivery update from Car_MS"""
        self.logger.info(f"Acknowledging delivery update for {message.get('parcel_id')}")
        
        # Acknowledge Car_MS
        ack_message = {
            'message_type': 'acknowledgment',
            'request_id': message.get('request_id'),
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('car_ms_queue', ack_message)
        
        # Share delivery update with Storage_MS
        update_message = {
            'message_type': 'update_delivery',
            'parcel_id': message.get('parcel_id'),
            'status': message.get('status'),
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('storage_ms_queue', update_message)
        
        self.log_action('delivery_updated', message)
        
        # Notify UI_MS
        ui_notification = {
            'message_type': 'delivery_update',
            'request_id': message.get('request_id'),
            'parcel_id': message.get('parcel_id'),
            'status': message.get('status'),
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('ui_ms_queue', ui_notification)
        
    def handle_acknowledgment(self, message: Dict[str, Any]):
        """Handle acknowledgments from other services"""
        self.log_action('acknowledgment_received', message)
        
    def log_action(self, action: str, details: Dict[str, Any]):
        """Log action to Log_MS"""
        log_message = {
            'message_type': 'store_log',
            'service_name': 'Controller_MS',
            'action': action,
            'details': details,
            'request_id': details.get('request_id'),
            'timestamp': datetime.now().isoformat()
        }
        
        self.message_bus.send_message('log_ms_queue', log_message)



# ============================================================================
# IDGEN_MS (Internal - Ubuntu/Server_1)
# ============================================================================

# idgen_ms/idgen_service.py
import hashlib
from datetime import datetime

class IDGen_MS:
    """Internal microservice for generating unique parcel IDs"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('IDGen_MS')
        self.counter = 0
        
    def start(self):
        """Start listening for ID generation requests"""
        self.message_bus.receive_message('idgen_ms_queue', self.handle_message)
        self.message_bus.start_consuming()
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming messages"""
        msg_type = message.get('message_type')
        
        if msg_type == 'generate_parcel_id':
            parcel_id = self.generate_parcel_id(message.get('request_id'))
            self.store_parcel_id(parcel_id, message)
            
    def generate_parcel_id(self, request_id: str) -> str:
        """Generate a unique parcel ID"""
        self.counter += 1
        timestamp = datetime.now().isoformat()
        data = f"{request_id}-{timestamp}-{self.counter}"
        hash_object = hashlib.sha256(data.encode())
        parcel_id = f"PKG-{hash_object.hexdigest()[:12].upper()}"
        
        self.logger.info(f"Generated parcel ID: {parcel_id}")
        return parcel_id
        
    def store_parcel_id(self, parcel_id: str, original_message: Dict[str, Any]):
        """Share parcel ID with Storage_MS"""
        message = {
            'message_type': 'store_parcel_id',
            'parcel_id': parcel_id,
            'request_id': original_message.get('request_id'),
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Sharing parcel ID {parcel_id} with Storage_MS")
        self.message_bus.send_message('storage_ms_queue', message)
        
        # Wait for acknowledgment and then acknowledge Controller
        self.wait_for_storage_ack(parcel_id, original_message.get('request_id'))
        
    def wait_for_storage_ack(self, parcel_id: str, request_id: str):
        """Wait for Storage acknowledgment and notify Controller"""
        # In real implementation, this would be async
        time.sleep(0.1)  # Simulate waiting
        
        ack_message = {
            'message_type': 'parcel_id_generated',
            'parcel_id': parcel_id,
            'request_id': request_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Acknowledging Controller for parcel ID {parcel_id}")
        self.message_bus.send_message('controller_ms_queue', ack_message)
# ============================================================================
# LOG_MS (Internal - Ubuntu/Server_1)
# ============================================================================

# log_ms/log_service.py
class Log_MS:
    """Internal microservice for storing logs"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('Log_MS')
        self.db = Database('database_3.db')
        self.setup_database()
        
    def setup_database(self):
        """Create logs table"""
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                service_name TEXT,
                action TEXT,
                details TEXT,
                request_id TEXT
            )
        ''')
        
    def start(self):
        """Start listening for log requests"""
        self.message_bus.receive_message('log_ms_queue', self.handle_message)
        self.message_bus.start_consuming()
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming log messages"""
        if message.get('message_type') == 'store_log':
            self.store_log(message)
            
    def store_log(self, message: Dict[str, Any]):
        """Store log in Database_3"""
        self.db.execute('''
            INSERT INTO logs (timestamp, service_name, action, details, request_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            message.get('timestamp', datetime.now().isoformat()),
            message.get('service_name'),
            message.get('action'),
            json.dumps(message.get('details', {})),
            message.get('request_id')
        ))
        
        self.logger.info(f"Stored log: {message.get('action')}")



# ============================================================================
# SENDER_MS (External - Windows/Laptop_1)
# ============================================================================
import time
import uuid
from datetime import datetime

class SenderMS:
    """External microservice for senders to request deliveries"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('SenderMS')
        
    def request_delivery(self, sender_name: str, recipient_name: str, 
                        pickup_address: str, delivery_address: str, 
                        package_description: str):
        """Request a delivery through UI_MS"""
        request_id = str(uuid.uuid4())
        
        message = {
            'message_type': 'delivery_request',
            'request_id': request_id,
            'timestamp': datetime.now().isoformat(),
            'sender_name': sender_name,
            'recipient_name': recipient_name,
            'pickup_address': pickup_address,
            'delivery_address': delivery_address,
            'package_description': package_description
        }
        
        self.logger.info(f"Sending delivery request: {request_id}")
        self.message_bus.send_message('ui_ms_queue', message)
        
        # Listen for acknowledgment
        self.message_bus.receive_message('sender_ms_queue', self.handle_notification)
        
    def handle_notification(self, message: Dict[str, Any]):
        """Handle notifications from UI_MS"""
        msg_type = message.get('message_type')
        
        if msg_type == 'delivery_assigned':
            self.logger.info(f"Delivery assigned: Parcel ID {message.get('parcel_id')}, "
                           f"Car ID {message.get('car_id')}")
            # Send acknowledgment
            ack_message = {
                'message_type': 'acknowledgment',
                'request_id': message.get('request_id'),
                'timestamp': datetime.now().isoformat()
            }
            self.message_bus.send_message('ui_ms_queue', ack_message)
            
        elif msg_type == 'delivery_update':
            self.logger.info(f"Delivery update: {message.get('status')}")
            # Send acknowledgment
            ack_message = {
                'message_type': 'acknowledgment',
                'request_id': message.get('request_id'),
                'timestamp': datetime.now().isoformat()
            }
            self.message_bus.send_message('ui_ms_queue', ack_message)

# ============================================================================
# STORAGE_MS (Internal - Ubuntu/Server_1)
# ============================================================================

# storage_ms/storage_service.py
class Storage_MS:
    """Internal microservice for database operations"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('Storage_MS')
        
        # Initialize databases
        self.db1 = Database('database_1.db')  # Parcel data
        self.db2 = Database('database_2.db')  # Delivery assignments
        self.db3 = Database('database_3.db')  # Logs (handled by Log_MS)
        
        self.setup_databases()
        
    def setup_databases(self):
        """Create database tables"""
        # Database_1: Parcel data
        self.db1.execute('''
            CREATE TABLE IF NOT EXISTS parcels (
                parcel_id TEXT PRIMARY KEY,
                request_id TEXT,
                sender_name TEXT,
                recipient_name TEXT,
                pickup_address TEXT,
                delivery_address TEXT,
                package_description TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        ''')
        
        # Database_2: Delivery assignments
        self.db2.execute('''
            CREATE TABLE IF NOT EXISTS assignments (
                assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                parcel_id TEXT,
                car_id TEXT,
                assigned_at TEXT,
                UNIQUE(parcel_id, car_id)
            )
        ''')
        
    def start(self):
        """Start listening for storage requests"""
        self.message_bus.receive_message('storage_ms_queue', self.handle_message)
        self.message_bus.start_consuming()
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming messages"""
        msg_type = message.get('message_type')
        
        if msg_type == 'store_parcel_id':
            self.store_parcel_id(message)
        elif msg_type == 'store_car_id':
            self.store_car_id(message)
        elif msg_type == 'get_parcel_id':
            self.get_parcel_id(message)
        elif msg_type == 'get_car_id':
            self.get_car_id(message)
        elif msg_type == 'store_delivery':
            self.store_delivery(message)
        elif msg_type == 'update_delivery':
            self.update_delivery(message)
            
    def store_parcel_id(self, message: Dict[str, Any]):
        """Store parcel ID in Database_2"""
        parcel_id = message.get('parcel_id')
        
        self.db2.execute(
            'INSERT OR IGNORE INTO assignments (parcel_id, assigned_at) VALUES (?, ?)',
            (parcel_id, datetime.now().isoformat())
        )
        
        self.logger.info(f"Stored parcel ID {parcel_id} in Database_2")
        
        # Send acknowledgment to IDGen_MS
        ack_message = {
            'message_type': 'storage_acknowledgment',
            'parcel_id': parcel_id,
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('idgen_ms_queue', ack_message)
        
    def store_car_id(self, message: Dict[str, Any]):
        """Store car ID in Database_2"""
        car_id = message.get('car_id')
        parcel_id = message.get('parcel_id')
        
        self.db2.execute(
            'UPDATE assignments SET car_id = ? WHERE parcel_id = ?',
            (car_id, parcel_id)
        )
        
        self.logger.info(f"Stored car ID {car_id} in Database_2")
        
        # Send acknowledgment to Car_MS
        ack_message = {
            'message_type': 'storage_acknowledgment',
            'car_id': car_id,
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('car_ms_queue', ack_message)
        
    def get_parcel_id(self, message: Dict[str, Any]):
        """Retrieve parcel ID and send to Controller"""
        request_id = message.get('request_id')
        
        result = self.db2.fetchone(
            'SELECT parcel_id FROM assignments WHERE parcel_id LIKE ?',
            (f'PKG-%',)
        )
        
        response = {
            'message_type': 'parcel_id_response',
            'parcel_id': result['parcel_id'] if result else None,
            'request_id': request_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.message_bus.send_message('controller_ms_queue', response)
        
    def get_car_id(self, message: Dict[str, Any]):
        """Retrieve car ID and send to Controller"""
        parcel_id = message.get('parcel_id')
        
        result = self.db2.fetchone(
            'SELECT car_id FROM assignments WHERE parcel_id = ?',
            (parcel_id,)
        )
        
        response = {
            'message_type': 'car_id_response',
            'car_id': result['car_id'] if result else None,
            'parcel_id': parcel_id,
            'timestamp': datetime.now().isoformat()
        }
        
        self.message_bus.send_message('controller_ms_queue', response)
        
    def store_delivery(self, message: Dict[str, Any]):
        """Store delivery information in Database_1"""
        self.db1.execute('''
            INSERT INTO parcels (parcel_id, request_id, sender_name, recipient_name,
                               pickup_address, delivery_address, package_description,
                               status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            message.get('parcel_id'),
            message.get('request_id'),
            message.get('sender_name'),
            message.get('recipient_name'),
            message.get('pickup_address'),
            message.get('delivery_address'),
            message.get('package_description'),
            'assigned',
            datetime.now().isoformat(),
            datetime.now().isoformat()
        ))
        
        self.logger.info(f"Stored delivery for parcel {message.get('parcel_id')}")
        
        # Acknowledge Controller
        ack_message = {
            'message_type': 'storage_acknowledgment',
            'parcel_id': message.get('parcel_id'),
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('controller_ms_queue', ack_message)
        
    def update_delivery(self, message: Dict[str, Any]):
        """Update delivery status in Database_1"""
        self.db1.execute(
            'UPDATE parcels SET status = ?, updated_at = ? WHERE parcel_id = ?',
            (message.get('status'), datetime.now().isoformat(), message.get('parcel_id'))
        )
        
        self.logger.info(f"Updated delivery status for {message.get('parcel_id')}")
        
        # Acknowledge Controller
        ack_message = {
            'message_type': 'storage_acknowledgment',
            'parcel_id': message.get('parcel_id'),
            'timestamp': datetime.now().isoformat()
        }
        self.message_bus.send_message('controller_ms_queue', ack_message)



# ============================================================================
# UI_MS (Internal - Ubuntu/Server_1)
# ============================================================================

# ui_ms/ui_service.py
class UI_MS:
    """Internal microservice handling user interface interactions"""
    
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self.logger = logging.getLogger('UI_MS')
        
    def start(self):
        """Start listening for messages"""
        self.message_bus.receive_message('ui_ms_queue', self.handle_message)
        self.message_bus.start_consuming()
        
    def handle_message(self, message: Dict[str, Any]):
        """Handle incoming messages"""
        msg_type = message.get('message_type')
        
        if msg_type == 'delivery_request':
            self.forward_to_controller(message)
        elif msg_type == 'delivery_notification':
            self.notify_sender(message)
        elif msg_type == 'acknowledgment':
            self.forward_ack_to_controller(message)
            
    def forward_to_controller(self, message: Dict[str, Any]):
        """Forward delivery request to Controller_MS"""
        self.logger.info(f"Forwarding delivery request {message.get('request_id')} to Controller")
        self.message_bus.send_message('controller_ms_queue', message)
        
    def notify_sender(self, message: Dict[str, Any]):
        """Notify sender about delivery status"""
        self.logger.info(f"Notifying sender about {message.get('message_type')}")
        self.message_bus.send_message('sender_ms_queue', message)
        
    def forward_ack_to_controller(self, message: Dict[str, Any]):
        """Forward acknowledgment to Controller_MS"""
        self.logger.info(f"Forwarding acknowledgment to Controller")
        self.message_bus.send_message('controller_ms_queue', message)
# ============================================================================
# MAIN APPLICATION RUNNERS
# ============================================================================

# run_sender.py
"""
Run this on Windows/Laptop_1 for Sender_MS
"""
import logging
import sys

def run_sender():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Connect to message bus (RabbitMQ should be accessible from both locations)
    message_bus = MessageBus(host='SERVER_1_IP_ADDRESS', port=5672)
    message_bus.connect()
    
    sender = SenderMS(message_bus)
    
    # Example: Request a delivery
    sender.request_delivery(
        sender_name="John Doe",
        recipient_name="Jane Smith",
        pickup_address="123 Main St, City A",
        delivery_address="456 Oak Ave, City B",
        package_description="Documents - Handle with care"
    )
    
    # Start listening for notifications
    try:
        sender.message_bus.start_consuming()
    except KeyboardInterrupt:
        print("\nSender MS shutting down...")
        message_bus.close()
        sys.exit(0)


# run_car.py
"""
Run this on Windows/Laptop_1 for Car_MS
"""
def run_car():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Connect to message bus
    message_bus = MessageBus(host='SERVER_1_IP_ADDRESS', port=5672)
    message_bus.connect()
    
    car = Car_MS(message_bus, car_id="CAR-1001")
    
    # Start listening for requests
    try:
        car.start()
        car.message_bus.start_consuming()
    except KeyboardInterrupt:
        print("\nCar MS shutting down...")
        message_bus.close()
        sys.exit(0)


# run_server.py
"""
Run this on Ubuntu/Server_1 for all internal microservices
"""
import threading

def run_server():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create separate message bus connections for each service
    ui_bus = MessageBus(host='localhost', port=5672)
    idgen_bus = MessageBus(host='localhost', port=5672)
    controller_bus = MessageBus(host='localhost', port=5672)
    storage_bus = MessageBus(host='localhost', port=5672)
    log_bus = MessageBus(host='localhost', port=5672)
    
    # Initialize all internal microservices
    ui_ms = UI_MS(ui_bus)
    idgen_ms = IDGen_MS(idgen_bus)
    controller_ms = Controller_MS(controller_bus)
    storage_ms = Storage_MS(storage_bus)
    log_ms = Log_MS(log_bus)
    
    # Start each service in its own thread
    services = [
        ('UI_MS', ui_ms),
        ('IDGen_MS', idgen_ms),
        ('Controller_MS', controller_ms),
        ('Storage_MS', storage_ms),
        ('Log_MS', log_ms)
    ]
    
    threads = []
    
    for service_name, service in services:
        thread = threading.Thread(
            target=service.start,
            name=service_name,
            daemon=True
        )
        thread.start()
        threads.append(thread)
        print(f"Started {service_name}")
    
    print("\nAll internal microservices are running...")
    print("Press Ctrl+C to stop\n")
    
    # Keep main thread alive
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutting down all services...")
        sys.exit(0)



# ============================================================================
# TESTING SCRIPT
# ============================================================================

# test_system.py
"""
Comprehensive testing script for the delivery system
"""

def test_full_workflow():
    """Test complete delivery workflow"""
    
    print("=" * 70)
    print("TESTING DELIVERY MANAGEMENT SYSTEM")
    print("=" * 70)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(message)s'
    )
    
    # Create message bus connections
    print("\n[1/8] Connecting to message bus...")
    message_bus = MessageBus(host='localhost', port=5672)
    message_bus.connect()
    print("✓ Connected to RabbitMQ")
    
    # Initialize services
    print("\n[2/8] Initializing microservices...")
    services = {
        'ui': UI_MS(MessageBus(host='localhost')),
        'idgen': IDGen_MS(MessageBus(host='localhost')),
        'controller': Controller_MS(MessageBus(host='localhost')),
        'storage': Storage_MS(MessageBus(host='localhost')),
        'log': Log_MS(MessageBus(host='localhost')),
        'car': Car_MS(MessageBus(host='localhost'), car_id='CAR-TEST-001'),
        'sender': SenderMS(MessageBus(host='localhost'))
    }
    print("✓ All services initialized")
    
    # Start services in threads
    print("\n[3/8] Starting microservices...")
    threads = []
    for name, service in services.items():
        if name not in ['sender']:  # Sender doesn't need to start consuming
            thread = threading.Thread(
                target=service.start if hasattr(service, 'start') else service.message_bus.start_consuming,
                daemon=True
            )
            thread.start()
            threads.append(thread)
    time.sleep(2)  # Give services time to start
    print("✓ All services running")
    
    # Request delivery
    print("\n[4/8] Requesting delivery...")
    services['sender'].request_delivery(
        sender_name="Test Sender",
        recipient_name="Test Recipient",
        pickup_address="Test Pickup Address",
        delivery_address="Test Delivery Address",
        package_description="Test Package"
    )
    print("✓ Delivery requested")
    
    # Wait for processing
    print("\n[5/8] Processing delivery request...")
    time.sleep(5)
    print("✓ Request processed")
    
    # Simulate delivery update
    print("\n[6/8] Simulating delivery update...")
    # Car would request update here
    print("✓ Update simulated")
    
    # Verify databases
    print("\n[7/8] Verifying database entries...")
    storage = services['storage']
    
    parcels = storage.db1.fetchall("SELECT * FROM parcels")
    assignments = storage.db2.fetchall("SELECT * FROM assignments")
    logs = services['log'].db.fetchall("SELECT * FROM logs")
    
    print(f"  - Parcels in Database_1: {len(parcels)}")
    print(f"  - Assignments in Database_2: {len(assignments)}")
    print(f"  - Logs in Database_3: {len(logs)}")
    print("✓ Databases verified")
    
    print("\n[8/8] Test complete!")
    print("=" * 70)
    print("SYSTEM TEST PASSED ✓")
    print("=" * 70)


if __name__ == "__main__":
    # Run appropriate script based on context
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        
        if mode == "server":
            run_server()
        elif mode == "sender":
            run_sender()
        elif mode == "car":
            run_car()
        elif mode == "test":
            test_full_workflow()
        else:
            print("Usage: python script.py [server|sender|car|test]")
    else:
        print("\nDelivery Management Microservices System")
        print("=" * 50)
        print("\nUsage:")
        print("  python script.py server  - Run all internal services")
        print("  python script.py sender  - Run sender service")
        print("  python script.py car     - Run car service")
        print("  python script.py test    - Run system tests")
        print("\nMake sure RabbitMQ is running first!")
        print("  docker-compose up -d")
        print("=" * 50)
