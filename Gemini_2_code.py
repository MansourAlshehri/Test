import time
import uuid
import json
from collections import defaultdict

# --- YAML Messaging & Database Simulation Helpers ---

# In a real environment, this would be an actual YAML serialization/deserialization.
# Here, we use a simple dict structure and print it nicely to represent the YAML format.
def create_yaml_message(sender, recipient, action, data=None):
    """Simulates creating a YAML-based message (as a Python dictionary)."""
    if data is None:
        data = {}
    
    message = {
        'timestamp': time.time(),
        'sender': sender,
        'recipient': recipient,
        'action': action,
        'data': data
    }
    # Simulate YAML output for clarity
    yaml_representation = f"""
---
sender: {message['sender']}
recipient: {message['recipient']}
action: {message['action']}
data: {json.dumps(message['data'], indent=4)}
...
"""
    return message, yaml_representation

# Simulating the three internal databases using in-memory Python dicts
class DatabaseSimulator:
    def __init__(self):
        # Database_1: Stores complete parcel delivery data
        self.parcel_data = defaultdict(dict) 
        # Database_2: Stores assignment data (Parcel ID, Car ID linking)
        self.delivery_assignment = defaultdict(dict)
        # Database_3: Stores all system logs
        self.logs = []
        print("\n[INIT] 3 Internal Databases (In-Memory) initialized.")

DB = DatabaseSimulator()


# --- 1. Internal Microservices (Ubuntu/Server_1) ---

class Log_MS:
    """Handles logging and stores data in Database_3."""
    NAME = "Log_MS"
    
    def store_log(self, message):
        log_entry = {
            'time': time.strftime("%Y-%m-%d %H:%M:%S"),
            'log_data': message['data']
        }
        DB.logs.append(log_entry)
        print(f"  [{self.NAME}]: Stored log in Database_3. Total logs: {len(DB.logs)}")
        # No formal acknowledgement required, as logging is usually non-blocking.


class Storage_MS:
    """Handles all data persistence for Database_1 and Database_2."""
    NAME = "Storage_MS"

    def __init__(self, log_ms):
        self.log_ms = log_ms

    def store_parcel_id(self, message):
        parcel_id = message['data']['parcel_id']
        DB.delivery_assignment['parcels'][parcel_id] = {'id': parcel_id, 'status': 'ID_GENERATED'}
        print(f"  [{self.NAME}]: Stored Parcel ID '{parcel_id}' in Database_2 (Assignment).")
        
        # Acknowledge the sender (IDGen_MS)
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_PARCEL_ID_STORED', {'status': 'OK'})
        return response

    def store_car_id(self, message):
        car_id = message['data']['car_id']
        parcel_id = message['data']['parcel_id'] # Assuming IDGen_MS provided this context
        
        # Link the Car ID to the Parcel ID in Database_2
        DB.delivery_assignment['parcels'][parcel_id]['car_id'] = car_id
        DB.delivery_assignment['parcels'][parcel_id]['status'] = 'CAR_ASSIGNED'
        print(f"  [{self.NAME}]: Stored Car ID '{car_id}' for Parcel '{parcel_id}' in Database_2.")

        # Acknowledge the sender (Car_MS)
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_CAR_ID_STORED', {'status': 'OK'})
        return response

    def get_data(self, message):
        requested_type = message['data']['request_type']
        parcel_id = message['data'].get('parcel_id')
        car_id = message['data'].get('car_id')
        
        data = None
        if requested_type == 'parcel_id' and parcel_id:
            data = DB.delivery_assignment['parcels'].get(parcel_id)
            print(f"  [{self.NAME}]: Retrieved Parcel ID data for '{parcel_id}' from Database_2.")
        elif requested_type == 'car_id' and parcel_id: # Car ID is stored against the parcel in this model
            data = DB.delivery_assignment['parcels'].get(parcel_id, {}).get('car_id')
            print(f"  [{self.NAME}]: Retrieved Car ID for Parcel '{parcel_id}' from Database_2.")

        response, _ = create_yaml_message(self.NAME, message['sender'], f'RESPONSE_{requested_type.upper()}', {'data': data})
        return response

    def store_delivery(self, message):
        delivery_data = message['data']['delivery_data']
        parcel_id = delivery_data['parcel_id']
        
        # Store full delivery data in Database_1
        DB.parcel_data[parcel_id] = delivery_data
        DB.delivery_assignment['parcels'][parcel_id]['status'] = 'DELIVERY_ASSIGNED'
        print(f"  [{self.NAME}]: Stored full Delivery data for Parcel '{parcel_id}' in Database_1.")
        
        # Acknowledge the sender (Controller_MS)
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_DELIVERY_STORED', {'status': 'OK'})
        return response
    
    def update_delivery(self, message):
        parcel_id = message['data']['parcel_id']
        update_info = message['data']['update']
        
        if parcel_id in DB.parcel_data:
            DB.parcel_data[parcel_id].update(update_info)
            DB.delivery_assignment['parcels'][parcel_id]['status'] = update_info['status']
            print(f"  [{self.NAME}]: Updated delivery status for Parcel '{parcel_id}' in Database_1 to: {update_info['status']}.")

            # Acknowledge the sender (Controller_MS)
            response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_DELIVERY_UPDATED', {'status': 'OK'})
        else:
            print(f"  [{self.NAME}]: ERROR: Parcel '{parcel_id}' not found for update.")
            response, _ = create_yaml_message(self.NAME, message['sender'], 'ERROR_NOT_FOUND', {'status': 'FAIL', 'message': f'Parcel {parcel_id} not found'})
            
        return response


class IDGen_MS:
    """Generates unique Parcel IDs."""
    NAME = "IDGen_MS"
    
    def __init__(self, storage_ms):
        self.storage_ms = storage_ms
        self.counter = 100000

    def generate_parcel_id(self, message):
        self.counter += 1
        new_id = f"PRCL-{self.counter}"
        print(f"  [{self.NAME}]: Generated new Parcel ID: {new_id}")
        
        # 1. IDGen_MS shares parcel ID with Storage_MS
        msg_to_storage, yaml_to_storage = create_yaml_message(self.NAME, self.storage_ms.NAME, 'STORE_PARCEL_ID', {'parcel_id': new_id, 'context_data': message['data']})
        print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_to_storage['action']}")
        
        # 2. Storage_MS stores parcel ID in Database_2 & 3. Storage_MS acknowledges IDGen_MS
        ack_from_storage = self.storage_ms.store_parcel_id(msg_to_storage)
        print(f"  [{self.storage_ms.NAME}] <- [{self.NAME}]: {ack_from_storage['action']}")

        # 4. IDGen_MS acknowledges Controller_MS
        response, _ = create_yaml_message(self.NAME, message['sender'], 'PARCEL_ID_GENERATED', {'parcel_id': new_id})
        return response


class UI_MS:
    """Front-end interaction layer."""
    NAME = "UI_MS"
    
    def __init__(self, controller_ms):
        self.controller_ms = controller_ms

    def request_delivery(self, message):
        print(f"  [{self.NAME}]: Received delivery request from {message['sender']}.")
        
        # UI_MS forwards ‘request delivery’ to Controller_MS
        msg_to_controller, _ = create_yaml_message(self.NAME, self.controller_ms.NAME, 'PROCESS_DELIVERY_REQUEST', message['data'])
        print(f"  [{self.NAME}] -> [{self.controller_ms.NAME}]: {msg_to_controller['action']}")
        
        # Controller_MS orchestrates the entire process and returns the final acknowledgement
        ack_from_controller = self.controller_ms.orchestrate_delivery_request(msg_to_controller)

        # UI_MS acknowledges Sender_MS
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_DELIVERY_PROCESSED', ack_from_controller['data'])
        return response
    
    def notify_sender(self, message):
        # UI_MS notifies Sender_MS
        msg_to_sender, _ = create_yaml_message(self.NAME, 'Sender_MS', 'DELIVERY_UPDATE', message['data'])
        print(f"  [{self.NAME}] -> [Sender_MS]: {msg_to_sender['action']}")
        
        # Sender_MS acknowledges UI_MS
        ack_from_sender = Sender_MS.acknowledge(msg_to_sender)
        print(f"  [Sender_MS] <- [{self.NAME}]: {ack_from_sender['action']}")

        # UI_MS acknowledges Controller_MS
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_NOTIFICATION_COMPLETE', ack_from_sender['data'])
        return response


class Controller_MS:
    """The central orchestrator of the delivery process."""
    NAME = "Controller_MS"

    def __init__(self, idgen_ms, storage_ms, log_ms, car_ms):
        self.idgen_ms = idgen_ms
        self.storage_ms = storage_ms
        self.log_ms = log_ms
        self.car_ms = car_ms
        self.ui_ms = None # UI_MS needs to be set after initialization due to circular dependency

    def set_ui_ms(self, ui_ms):
        self.ui_ms = ui_ms

    def log(self, sender, action, details):
        log_msg, _ = create_yaml_message(sender, self.log_ms.NAME, 'STORE_LOG', {'action': action, 'details': details})
        self.log_ms.store_log(log_msg)

    def orchestrate_delivery_request(self, request_msg):
        # 1. Controller_MS requests parcel ID from IDGen_MS
        self.log(self.NAME, 'Delivery Request Started', request_msg['data'])
        print(f"\n[{self.NAME}] Orchestration Start: Parcel ID Generation")
        
        msg_to_idgen, _ = create_yaml_message(self.NAME, self.idgen_ms.NAME, 'GENERATE_PARCEL_ID', request_msg['data'])
        print(f"  [{self.NAME}] -> [{self.idgen_ms.NAME}]: {msg_to_idgen['action']}")
        
        # IDGen_MS flow (Generates ID, stores in DB2 via Storage_MS, Acknowledges Controller_MS)
        id_response = self.idgen_ms.generate_parcel_id(msg_to_idgen)
        print(f"  [{self.idgen_ms.NAME}] <- [{self.NAME}]: {id_response['action']}")
        parcel_id = id_response['data']['parcel_id']
        self.log(self.NAME, 'Parcel ID Generated', {'parcel_id': parcel_id})


        # 2. Controller_MS requests car ID from Car_MS
        print(f"\n[{self.NAME}] Orchestration Step: Car Assignment")
        # For simulation, we send the context (parcel ID) to Car_MS
        msg_to_car, _ = create_yaml_message(self.NAME, self.car_ms.NAME, 'REQUEST_CAR_ID', {'location': request_msg['data']['pickup_location'], 'parcel_id': parcel_id})
        print(f"  [{self.NAME}] -> [{self.car_ms.NAME}]: {msg_to_car['action']}")
        
        # Car_MS flow (Checks ID, shares with Storage_MS, Acknowledges Controller_MS)
        car_response = self.car_ms.check_and_assign_car(msg_to_car)
        print(f"  [{self.car_ms.NAME}] <- [{self.NAME}]: {car_response['action']}")
        car_id = car_response['data']['car_id']
        self.log(self.NAME, 'Car ID Confirmed', {'parcel_id': parcel_id, 'car_id': car_id})

        
        # 3. Controller_MS retrieves final assignment data
        print(f"\n[{self.NAME}] Orchestration Step: Final Assignment Data Collection")
        
        # Controller_MS requests parcel ID from Storage_MS (Retrieving data stored in IDGen_MS step)
        msg_get_parcel_id, _ = create_yaml_message(self.NAME, self.storage_ms.NAME, 'GET_DATA', {'request_type': 'parcel_id', 'parcel_id': parcel_id})
        print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_get_parcel_id['action']} (Parcel ID)")
        parcel_data_response = self.storage_ms.get_data(msg_get_parcel_id)
        # Assuming parcel_data_response['data'] is the full assignment record from DB2
        
        # Controller_MS requests car ID from Storage_MS (Retrieving data stored in Car_MS step)
        msg_get_car_id, _ = create_yaml_message(self.NAME, self.storage_ms.NAME, 'GET_DATA', {'request_type': 'car_id', 'parcel_id': parcel_id})
        print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_get_car_id['action']} (Car ID)")
        car_id_response = self.storage_ms.get_data(msg_get_car_id)
        
        
        # 4. Controller_MS assigns delivery & stores
        
        # Controller_MS assigns delivery (Logic step)
        final_delivery_data = {
            'parcel_id': parcel_id,
            'car_id': car_id_response['data']['data'],
            'origin': request_msg['data']['pickup_location'],
            'destination': request_msg['data']['delivery_location'],
            'status': 'ASSIGNED_PENDING_PICKUP',
            'created_at': time.time()
        }
        
        # Controller_MS shares delivery with Storage_MS
        msg_to_store, _ = create_yaml_message(self.NAME, self.storage_ms.NAME, 'STORE_DELIVERY', {'delivery_data': final_delivery_data})
        print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_to_store['action']}")
        
        # Storage_MS stores delivery in Database_1 & Acknowledges Controller_MS
        ack_from_store = self.storage_ms.store_delivery(msg_to_store)
        print(f"  [{self.storage_ms.NAME}] <- [{self.NAME}]: {ack_from_store['action']}")
        self.log(self.NAME, 'Delivery Finalized and Stored', {'parcel_id': parcel_id, 'car_id': car_id})

        
        # 5. Controller_MS notifies Car_MS and UI_MS
        print(f"\n[{self.NAME}] Orchestration Step: Notifications")

        # Controller-MS notifies Car_MS
        msg_notify_car, _ = create_yaml_message(self.NAME, self.car_ms.NAME, 'NOTIFY_NEW_DELIVERY', {'parcel_id': parcel_id, 'car_id': final_delivery_data['car_id']})
        print(f"  [{self.NAME}] -> [{self.car_ms.NAME}]: {msg_notify_car['action']}")
        
        # Car_MS acknowledges Controller_MS
        ack_from_car = self.car_ms.acknowledge(msg_notify_car)
        print(f"  [{self.car_ms.NAME}] <- [{self.NAME}]: {ack_from_car['action']}")
        self.log(self.NAME, 'Car Notified', {'parcel_id': parcel_id})
        
        # Controller_MS notifies UI_MS
        msg_notify_ui, _ = create_yaml_message(self.NAME, self.ui_ms.NAME, 'DELIVERY_ASSIGNMENT_COMPLETE', {'parcel_id': parcel_id, 'status': final_delivery_data['status']})
        print(f"  [{self.NAME}] -> [{self.ui_ms.NAME}]: {msg_notify_ui['action']}")
        
        # UI_MS flow (Notifies Sender_MS, Acknowledges Controller_MS)
        final_ack_from_ui = self.ui_ms.notify_sender(msg_notify_ui)
        print(f"  [{self.ui_ms.NAME}] <- [{self.NAME}]: {final_ack_from_ui['action']}")
        self.log(self.NAME, 'UI and Sender Notified', {'parcel_id': parcel_id})

        
        # Final response back to UI_MS caller
        return final_ack_from_ui

    def process_delivery_update_request(self, request_msg):
        parcel_id = request_msg['data']['parcel_id']
        update_details = request_msg['data']['update_details']
        
        print(f"\n[{self.NAME}] Orchestration Start: Delivery Update for Parcel {parcel_id}")

        # Controller_MS acknowledges Car_MS
        ack_to_car, _ = create_yaml_message(self.NAME, self.car_ms.NAME, 'ACK_UPDATE_REQUEST_RECEIVED', {'status': 'PROCESSING'})
        print(f"  [{self.NAME}] -> [{self.car_ms.NAME}]: {ack_to_car['action']}")

        # Controller_MS shares delivery update with Storage_MS
        msg_to_storage, _ = create_yaml_message(self.NAME, self.storage_ms.NAME, 'UPDATE_DELIVERY_STATUS', {'parcel_id': parcel_id, 'update': update_details})
        print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_to_storage['action']}")
        
        # Storage_MS updates delivery in Database_1 & acknowledges Controller_MS
        ack_from_storage = self.storage_ms.update_delivery(msg_to_storage)
        print(f"  [{self.storage_ms.NAME}] <- [{self.NAME}]: {ack_from_storage['action']}")
        self.log(self.NAME, 'Delivery Status Updated in DB1', {'parcel_id': parcel_id, 'new_status': update_details['status']})


        # Controller_MS notifies UI_MS
        msg_notify_ui, _ = create_yaml_message(self.NAME, self.ui_ms.NAME, 'DELIVERY_STATUS_UPDATE', {'parcel_id': parcel_id, 'status': update_details['status']})
        print(f"  [{self.NAME}] -> [{self.ui_ms.NAME}]: {msg_notify_ui['action']}")

        # UI_MS flow (Notifies Sender_MS, Acknowledges Controller_MS)
        final_ack_from_ui = self.ui_ms.notify_sender(msg_notify_ui)
        print(f"  [{self.ui_ms.NAME}] <- [{self.NAME}]: {final_ack_from_ui['action']}")
        self.log(self.NAME, 'UI and Sender Notified of Update', {'parcel_id': parcel_id})
        
        return final_ack_from_ui


# --- 2. External Microservices (Windows/Laptop_1) ---

class Sender_MS:
    """The external service initiating the request."""
    NAME = "Sender_MS"
    
    @staticmethod
    def request_delivery(ui_ms, request_details):
        print(f"\n[{Sender_MS.NAME}] ACTION: Initiating Delivery Request.")
        # Sender_MS requests delivery from UI_MS
        msg_to_ui, yaml_msg = create_yaml_message(Sender_MS.NAME, ui_ms.NAME, 'REQUEST_DELIVERY', request_details)
        
        print("--- YAML Message Sent ---")
        print(yaml_msg)
        
        # UI_MS handles the request and returns the final acknowledgement
        final_ack = ui_ms.request_delivery(msg_to_ui)
        
        print(f"\n[{Sender_MS.NAME}] FINAL ACK: Received final confirmation for delivery. Status: {final_ack['data']['status']}")
        return final_ack['data']['parcel_id'] if 'parcel_id' in final_ack['data'] else None
        
    @staticmethod
    def acknowledge(message):
        # Sender_MS acknowledges UI_MS
        response, _ = create_yaml_message(Sender_MS.NAME, message['sender'], 'ACK_DELIVERY_UPDATE_RECEIVED', {'status': 'OK', 'parcel_id': message['data']['parcel_id']})
        return response


class Car_MS:
    """Simulates the vehicle service, external to the main server."""
    NAME = "Car_MS"
    
    def __init__(self, storage_ms):
        self.storage_ms = storage_ms
        self.status = 'READY'
        self.assigned_car_id = f"CAR-{uuid.uuid4().hex[:4].upper()}"

    def check_car_availability(self):
        # In a real scenario, this checks GPS, battery, driver status, etc.
        return self.assigned_car_id if self.status == 'READY' else None

    def check_and_assign_car(self, message):
        available_id = self.check_car_availability()
        parcel_id = message['data']['parcel_id']
        
        # Car_MS checks car ID
        if available_id:
            # Car_MS shares car ID with Storage_MS
            msg_to_storage, _ = create_yaml_message(self.NAME, self.storage_ms.NAME, 'STORE_CAR_ID', {'car_id': available_id, 'parcel_id': parcel_id})
            print(f"  [{self.NAME}] -> [{self.storage_ms.NAME}]: {msg_to_storage['action']} (Assigned: {available_id})")
            
            # Storage_MS stores car ID in Database_2 & Acknowledges Car_MS
            ack_from_storage = self.storage_ms.store_car_id(msg_to_storage)
            print(f"  [{self.storage_ms.NAME}] <- [{self.NAME}]: {ack_from_storage['action']}")
            
            # Car_MS acknowledges Controller_MS
            response, _ = create_yaml_message(self.NAME, message['sender'], 'CAR_ID_CONFIRMED', {'car_id': available_id, 'status': 'OK'})
            return response
        else:
            response, _ = create_yaml_message(self.NAME, message['sender'], 'ERROR_NO_CAR_AVAILABLE', {'status': 'FAIL'})
            return response

    def acknowledge(self, message):
        # Car_MS acknowledges Controller_MS (for notification)
        response, _ = create_yaml_message(self.NAME, message['sender'], 'ACK_DELIVERY_NOTIFICATION_RECEIVED', {'status': 'OK', 'car_id': self.assigned_car_id})
        return response
    
    def request_delivery_update(self, controller_ms, parcel_id, update_status):
        # Car_MS requests delivery update from Controller_MS
        print(f"\n[{self.NAME}] ACTION: Requesting delivery update for {parcel_id}. New status: {update_status}")
        msg_to_controller, _ = create_yaml_message(self.NAME, controller_ms.NAME, 'REQUEST_DELIVERY_STATUS_UPDATE', {'parcel_id': parcel_id, 'update_details': {'status': update_status}})
        
        # Controller_MS acknowledges Car_MS (immediately)
        ack_from_controller = controller_ms.process_delivery_update_request(msg_to_controller)
        
        print(f"  [{controller_ms.NAME}] <- [{self.NAME}]: {ack_from_controller['action']} (Final acknowledgement from UI_MS flow)")
        return ack_from_controller


# --- 3. System Initialization and Simulation ---

def run_simulation():
    # Setup Microservices
    Log = Log_MS()
    Storage = Storage_MS(Log)
    IDGen = IDGen_MS(Storage)
    Car = Car_MS(Storage)
    
    # Initialize Controller_MS (The orchestrator)
    Controller = Controller_MS(IDGen, Storage, Log, Car)
    
    # Initialize UI_MS (Handles request from Sender_MS)
    UI = UI_MS(Controller)
    Controller.set_ui_ms(UI) # Resolve circular dependency
    
    print("\n" + "="*80)
    print("                 MICROSERVICE DELIVERY SYSTEM SIMULATION START (YAML Communication)                 ")
    print("="*80)
    
    # --- Scenario 1: Sender_MS requests delivery and assignment is performed ---
    
    request_data = {
        'item_description': 'Emergency medical supply',
        'pickup_location': 'Laptop_1/Windows (Sender_MS Location)',
        'delivery_location': 'Server_1/Ubuntu (Destination)',
        'customer_ref': str(uuid.uuid4())
    }
    
    # Sender_MS starts the whole process
    assigned_parcel_id = Sender_MS.request_delivery(UI, request_data)
    
    print("\n" + "="*80)
    print(f"SCENARIO 1 COMPLETE: Delivery Assigned. Parcel ID: {assigned_parcel_id}")
    print("="*80)

    # --- Scenario 2: Car_MS updates the delivery status ---
    
    if assigned_parcel_id:
        # Car_MS requests delivery update from Controller_MS
        Car.request_delivery_update(Controller, assigned_parcel_id, "IN_TRANSIT_PICKED_UP")
        
        print("\n" + "="*80)
        print("SCENARIO 2 COMPLETE: Delivery Status Updated.")
        print("="*80)
    
    # --- Final Data Review (Simulating DB queries) ---
    
    print("\n[DB SIMULATION] Final State Check:")
    print(f"  > Total Logs in Database_3: {len(DB.logs)}")
    if assigned_parcel_id in DB.parcel_data:
        print("  > Parcel Data (Database_1 - Full Record):")
        print(json.dumps(DB.parcel_data[assigned_parcel_id], indent=4))
    
    if assigned_parcel_id in DB.delivery_assignment.get('parcels', {}):
        print("  > Assignment Data (Database_2 - Status Record):")
        print(json.dumps(DB.delivery_assignment['parcels'][assigned_parcel_id], indent=4))


if __name__ == "__main__":
    run_simulation()
