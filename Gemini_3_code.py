import uuid
import time
import json
from datetime import datetime

# --- Simulation Environment Setup ---
# Since we cannot use external libraries like Flask or actual PyYAML in this single file,
# we will simulate the YAML communication using Python dictionaries which represent
# the structure of a YAML payload, and a central dispatcher function.

# Global In-Memory Databases (Simulating Database_1, Database_2, Database_3)
# In a real environment, these would be external databases (e.g., MongoDB, PostgreSQL)
DATABASE_PARCEL_DATA = {} # Simulates Database_1 (Stores parcel records/delivery details)
DATABASE_DELIVERY_ASSIGNMENT = {} # Simulates Database_2 (Stores active assignments: Parcel ID, Car ID, Status)
DATABASE_LOGS = [] # Simulates Database_3 (Stores system activity logs)

# Microservice Endpoints/Handlers - A map to simulate routing requests
SERVICE_ENDPOINTS = {}

def simulate_yaml_exchange(sender_ms, target_ms, action, payload=None):
    """
    Simulates a network call between microservices using a dictionary structure
    that represents a YAML payload.

    Args:
        sender_ms (str): The name of the calling service.
        target_ms (str): The name of the receiving service.
        action (str): The method/API endpoint being called.
        payload (dict, optional): The data payload.
    """
    if payload is None:
        payload = {}

    request_payload = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "sender": sender_ms,
            "target": target_ms,
            "action": action
        },
        "data": payload
    }

    print(f"\n[{sender_ms} -> {target_ms}]: Request '{action}' with data: {payload}")

    # Simulate routing the request to the target service's handler
    if target_ms in SERVICE_ENDPOINTS and action in SERVICE_ENDPOINTS[target_ms]:
        handler = SERVICE_ENDPOINTS[target_ms][action]
        response_data = handler(request_payload)

        # Log the transaction (Logs are critical in MS architecture)
        log_ms_instance = Log_MS()
        log_data = {
            "source": sender_ms,
            "destination": target_ms,
            "action": action,
            "status": response_data.get('status', 'SUCCESS'),
            "details": response_data.get('message', 'Request processed.')
        }
        log_ms_instance.handle_log(log_data)

        return response_data
    else:
        error_response = {
            "status": "ERROR",
            "message": f"Endpoint not found: {target_ms}/{action}"
        }
        print(f"[{target_ms}]: Error - {error_response['message']}")
        return error_response


# --- Microservice Definitions ---

class Log_MS:
    """
    Internal MS: Stores system logs in Database_3.
    """
    def __init__(self):
        # Register the handler function to the simulated endpoint map
        if 'Log_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['Log_MS'] = {
                'store_log': self._store_log
            }

    def _store_log(self, request_payload):
        """Handler for the 'store_log' action."""
        log_entry = request_payload['data']
        log_entry['timestamp'] = datetime.now().isoformat()
        DATABASE_LOGS.append(log_entry)
        print(f"[Log_MS]: Stored log: {log_entry['action']} from {log_entry['source']}")
        return {"status": "ACK", "message": "Log stored successfully."}

    def handle_log(self, data):
        """Simulates Controller_MS sharing logs with Log_MS."""
        # This is the actual call Controller_MS would make
        log_data = {
            "source": data.get('source', 'Controller_MS'),
            "destination": data.get('destination', 'N/A'),
            "action": data.get('action', 'N/A'),
            "status": data.get('status', 'INFO'),
            "details": data.get('details', 'No details provided.')
        }

        # Simulate the call to its own endpoint (via the dispatcher)
        simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Log_MS",
            action='store_log',
            payload=log_data
        )


class Storage_MS:
    """
    Internal MS: Manages data persistence across Database_1 (Parcel Data)
    and Database_2 (Delivery Assignment).
    """
    def __init__(self):
        if 'Storage_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['Storage_MS'] = {
                'store_parcel_id': self._store_parcel_id,
                'store_car_id': self._store_car_id,
                'get_parcel_id': self._get_parcel_id,
                'get_car_id': self._get_car_id,
                'store_delivery_assignment': self._store_delivery_assignment,
                'update_delivery_status': self._update_delivery_status,
            }

    # --- Database_2 (Delivery Assignment) Operations ---
    def _store_parcel_id(self, request_payload):
        """Handler: IDGen_MS shares parcel ID with Storage_MS."""
        parcel_id = request_payload['data']['parcel_id']
        DATABASE_DELIVERY_ASSIGNMENT[parcel_id] = {'parcel_id': parcel_id, 'car_id': None, 'status': 'ID_GENERATED'}
        print(f"[Storage_MS]: Stored Parcel ID '{parcel_id}' in Database_2.")
        return {"status": "ACK", "message": "Parcel ID stored."}

    def _store_car_id(self, request_payload):
        """Handler: Car_MS shares car ID with Storage_MS."""
        parcel_id = request_payload['data']['parcel_id']
        car_id = request_payload['data']['car_id']

        # Assumption: The parcel ID is already in DB2 from IDGen_MS step
        if parcel_id in DATABASE_DELIVERY_ASSIGNMENT:
            DATABASE_DELIVERY_ASSIGNMENT[parcel_id]['car_id'] = car_id
            DATABASE_DELIVERY_ASSIGNMENT[parcel_id]['status'] = 'CAR_ASSIGNED_PENDING_DELIVERY'
            print(f"[Storage_MS]: Stored Car ID '{car_id}' for Parcel '{parcel_id}' in Database_2.")
            return {"status": "ACK", "message": "Car ID stored."}
        else:
            return {"status": "ERROR", "message": f"Parcel ID {parcel_id} not found in assignment database."}

    def _get_parcel_id(self, request_payload):
        """Handler: Controller_MS requests parcel ID from Storage_MS."""
        # For simplicity, we assume the Controller knows which assignment it is working on,
        # or we retrieve the latest ID. Let's retrieve a pending one for demonstration.
        for assignment in DATABASE_DELIVERY_ASSIGNMENT.values():
            if assignment.get('status') == 'CAR_ASSIGNED_PENDING_DELIVERY':
                print(f"[Storage_MS]: Shared Parcel ID '{assignment['parcel_id']}' with Controller_MS.")
                return {"status": "SUCCESS", "parcel_id": assignment['parcel_id']}
        return {"status": "ERROR", "message": "No pending parcel ID found."}

    def _get_car_id(self, request_payload):
        """Handler: Controller_MS requests car ID from Storage_MS."""
        # Same as above, retrieving the car ID associated with a pending assignment
        for assignment in DATABASE_DELIVERY_ASSIGNMENT.values():
            if assignment.get('status') == 'CAR_ASSIGNED_PENDING_DELIVERY' and assignment.get('car_id'):
                print(f"[Storage_MS]: Shared Car ID '{assignment['car_id']}' with Controller_MS.")
                return {"status": "SUCCESS", "car_id": assignment['car_id']}
        return {"status": "ERROR", "message": "No pending car ID found."}

    # --- Database_1 (Parcel Data) Operations ---
    def _store_delivery_assignment(self, request_payload):
        """Handler: Controller_MS shares delivery assignment with Storage_MS."""
        delivery_data = request_payload['data']
        parcel_id = delivery_data['parcel_id']
        car_id = delivery_data['car_id']
        
        DATABASE_PARCEL_DATA[parcel_id] = delivery_data
        
        # Update status in Database_2
        if parcel_id in DATABASE_DELIVERY_ASSIGNMENT:
            DATABASE_DELIVERY_ASSIGNMENT[parcel_id]['status'] = 'DELIVERY_ASSIGNED_DB1_STORED'

        print(f"[Storage_MS]: Stored full delivery assignment for Parcel '{parcel_id}' in Database_1.")
        return {"status": "ACK", "message": "Delivery assignment stored."}

    def _update_delivery_status(self, request_payload):
        """Handler: Controller_MS shares delivery update with Storage_MS."""
        parcel_id = request_payload['data']['parcel_id']
        new_status = request_payload['data']['new_status']

        if parcel_id in DATABASE_PARCEL_DATA:
            DATABASE_PARCEL_DATA[parcel_id]['status'] = new_status
            
            # Update status in Database_2 as well
            if parcel_id in DATABASE_DELIVERY_ASSIGNMENT:
                 DATABASE_DELIVERY_ASSIGNMENT[parcel_id]['status'] = new_status

            print(f"[Storage_MS]: Updated Parcel '{parcel_id}' status to '{new_status}' in Database_1 and Database_2.")
            return {"status": "ACK", "message": "Delivery status updated."}
        else:
            return {"status": "ERROR", "message": f"Parcel ID {parcel_id} not found in parcel data."}


class IDGen_MS:
    """
    Internal MS: Generates unique Parcel IDs and shares them with Storage_MS.
    """
    def __init__(self):
        if 'IDGen_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['IDGen_MS'] = {
                'request_parcel_id': self._generate_parcel_id
            }

    def _generate_parcel_id(self, request_payload):
        """Handler: Controller_MS requests parcel ID from IDGen_MS."""
        new_parcel_id = str(uuid.uuid4())[:8].upper()
        print(f"[IDGen_MS]: Generated Parcel ID: {new_parcel_id}")

        # IDGen_MS shares parcel ID with Storage_MS
        storage_ms_instance = Storage_MS()
        storage_response = simulate_yaml_exchange(
            sender_ms="IDGen_MS",
            target_ms="Storage_MS",
            action='store_parcel_id',
            payload={'parcel_id': new_parcel_id}
        )
        # Storage_MS acknowledges IDGen_MS (This response is stored in storage_response)

        if storage_response.get('status') == 'ACK':
            # IDGen_MS acknowledges Controller_MS
            return {"status": "SUCCESS", "parcel_id": new_parcel_id, "message": "Parcel ID generated and stored."}
        else:
            return {"status": "ERROR", "message": "Failed to store ID in Storage_MS."}

class Car_MS:
    """
    External MS: Simulates checking and managing car IDs/availability.
    """
    def __init__(self):
        if 'Car_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['Car_MS'] = {
                'request_car_id': self._check_car_id,
                'notify_delivery_assigned': self._handle_delivery_assigned,
                'request_delivery_update': self._get_delivery_update,
            }
        self.available_cars = {"CAR-001": "Online", "CAR-002": "Online"}

    def _check_car_id(self, request_payload):
        """Handler: Controller_MS requests car ID from Car_MS."""
        # Car_MS checks car ID (Simulate a check and return an available car)
        available_car = next(iter(self.available_cars.keys()), None)
        
        if available_car:
            # Car_MS shares car ID with Storage_MS
            parcel_id = request_payload['data'].get('parcel_id', 'TEMP_001') # Use a temp ID for assignment linkage
            storage_ms_instance = Storage_MS()
            storage_response = simulate_yaml_exchange(
                sender_ms="Car_MS",
                target_ms="Storage_MS",
                action='store_car_id',
                payload={'parcel_id': parcel_id, 'car_id': available_car}
            )
            # Storage_MS acknowledges Car_MS (This response is stored in storage_response)

            if storage_response.get('status') == 'ACK':
                # Car_MS acknowledges Controller_MS
                return {"status": "SUCCESS", "car_id": available_car, "message": "Car ID checked and assigned."}
            else:
                return {"status": "ERROR", "message": "Failed to store Car ID in Storage_MS."}
        else:
            return {"status": "ERROR", "message": "No cars available."}

    def _handle_delivery_assigned(self, request_payload):
        """Handler: Controller-MS notifies Car_MS about the assignment."""
        parcel_id = request_payload['data']['parcel_id']
        car_id = request_payload['data']['car_id']
        print(f"[Car_MS]: Notified of new delivery assignment: Parcel {parcel_id} to Car {car_id}.")
        # Car_MS acknowledges Controller_MS
        return {"status": "ACK", "message": "Delivery notification received."}

    def _get_delivery_update(self, request_payload):
        """Handler: Car_MS requests delivery update from Controller_MS."""
        # Car_MS acknowledges Controller_MS (handled by the dispatcher's log function)
        print(f"[Car_MS]: Requesting update for latest assigned delivery.")
        
        # Assuming the Car_MS knows which parcel it is asking about, 
        # but for simulation, we'll request a generic update.
        return {"status": "SUCCESS", "message": "Acknowledged. Awaiting update from Controller_MS."}


class UI_MS:
    """
    Internal MS: Handles initial user request and final notification to Sender_MS.
    """
    def __init__(self):
        if 'UI_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['UI_MS'] = {
                'request_delivery': self._forward_request_delivery,
                'notify_delivery_progress': self._notify_sender,
            }

    def _forward_request_delivery(self, request_payload):
        """Handler: Sender_MS requests delivery from UI_MS."""
        print(f"[UI_MS]: Received 'request delivery' from Sender_MS.")
        
        # UI_MS forwards 'request delivery' to Controller_MS
        controller_ms_instance = Controller_MS()
        controller_response = simulate_yaml_exchange(
            sender_ms="UI_MS",
            target_ms="Controller_MS",
            action='process_delivery_request',
            payload=request_payload['data'] # Forwarding original parcel details
        )
        return controller_response

    def _notify_sender(self, request_payload):
        """Handler: Controller_MS notifies UI_MS of delivery progress/completion."""
        print(f"[UI_MS]: Received notification from Controller_MS: {request_payload['data']['message']}")
        
        # UI_MS notifies Sender_MS (This is where the Sender_MS would receive its final update)
        sender_ms_instance = Sender_MS()
        sender_response = simulate_yaml_exchange(
            sender_ms="UI_MS",
            target_ms="Sender_MS",
            action='receive_notification',
            payload=request_payload['data']
        )
        
        # Sender_MS acknowledges UI_MS (This is sender_response)
        
        # UI_MS acknowledges Controller_MS
        return {"status": "ACK", "message": "Notification forwarded and acknowledged."}


class Sender_MS:
    """
    External MS: Initiates the process and receives final updates.
    """
    def __init__(self):
        if 'Sender_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['Sender_MS'] = {
                'receive_notification': self._acknowledge_notification,
            }

    def request_delivery(self, delivery_details):
        """Initiates the entire delivery process."""
        print(f"\n--- Sender_MS Initiating New Delivery Request ---")
        
        # Sender_MS requests delivery from UI_MS
        ui_response = simulate_yaml_exchange(
            sender_ms="Sender_MS",
            target_ms="UI_MS",
            action='request_delivery',
            payload=delivery_details
        )
        
        if ui_response.get('status') == 'SUCCESS':
            print(f"\n--- Sender_MS: Delivery Request SUCCESSFUL (Parcel ID: {ui_response.get('parcel_id')}) ---")
        else:
            print(f"\n--- Sender_MS: Delivery Request FAILED. Error: {ui_response.get('message')} ---")
        
        return ui_response

    def _acknowledge_notification(self, request_payload):
        """Handler: UI_MS notifies Sender_MS."""
        print(f"[Sender_MS]: Received notification: {request_payload['data']['message']}")
        # Sender_MS acknowledges UI_MS
        return {"status": "ACK", "message": "Notification received and acknowledged."}


class Controller_MS:
    """
    Internal MS: The core orchestrator managing the entire delivery assignment flow.
    """
    def __init__(self):
        if 'Controller_MS' not in SERVICE_ENDPOINTS:
            SERVICE_ENDPOINTS['Controller_MS'] = {
                'process_delivery_request': self._process_delivery_request,
                'request_delivery_update': self._handle_car_update_request,
            }
        # Initialize other services to ensure their endpoints are registered
        Storage_MS()
        IDGen_MS()
        Car_MS()
        UI_MS()
        Log_MS()

    # --- Major Flow 1: Delivery Assignment ---
    def _process_delivery_request(self, request_payload):
        """
        Handles the full 'request delivery' orchestration flow.
        """
        log_ms = Log_MS()
        storage_ms = Storage_MS()
        
        request_data = request_payload['data']
        temp_parcel_details = request_data.get('delivery_details', {}) # Contains destination, size, etc.

        # 1. Controller_MS requests parcel ID from IDGen_MS
        idgen_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="IDGen_MS",
            action='request_parcel_id',
            payload={}
        )
        
        # IDGen_MS acknowledges Controller_MS (This is idgen_response)
        log_ms.handle_log({'action': 'Parcel ID Request', 'status': idgen_response.get('status'), 'details': f"ID Gen Response: {idgen_response.get('parcel_id', 'N/A')}"})
        
        if idgen_response.get('status') != 'SUCCESS':
            return {"status": "ERROR", "message": "Failed to generate Parcel ID."}
        
        parcel_id = idgen_response['parcel_id']
        
        # 2. Controller_MS requests car ID from Car_MS
        # Pass the newly generated parcel_id to Car_MS so it can store it in DB2
        car_request_payload = {'parcel_id': parcel_id} 
        car_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Car_MS",
            action='request_car_id',
            payload=car_request_payload
        )
        
        # Car_MS acknowledges Controller_MS (This is car_response)
        log_ms.handle_log({'action': 'Car ID Request', 'status': car_response.get('status'), 'details': f"Car ID Response: {car_response.get('car_id', 'N/A')}"})
        
        if car_response.get('status') != 'SUCCESS':
            return {"status": "ERROR", "parcel_id": parcel_id, "message": "Failed to assign Car ID."}

        car_id = car_response['car_id']

        # 3. Controller_MS requests parcel ID from Storage_MS (Retrieving the confirmed ID)
        parcel_retrieval_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Storage_MS",
            action='get_parcel_id',
            payload={'lookup_key': parcel_id}
        )
        # Assuming parcel_id is confirmed: parcel_id = parcel_retrieval_response['parcel_id']
        
        # 4. Controller_MS requests car ID from Storage_MS (Retrieving the confirmed ID)
        car_retrieval_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Storage_MS",
            action='get_car_id',
            payload={'lookup_key': car_id}
        )
        # Assuming car_id is confirmed: car_id = car_retrieval_response['car_id']
        
        # 5. Controller_MS assigns delivery
        delivery_assignment_data = {
            'parcel_id': parcel_id,
            'car_id': car_id,
            'status': 'PENDING_PICKUP',
            'details': temp_parcel_details
        }
        
        # 6. Controller_MS shares delivery with Storage_MS
        storage_response_delivery = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Storage_MS",
            action='store_delivery_assignment',
            payload=delivery_assignment_data
        )
        
        # Storage_MS acknowledges Controller_MS (This is storage_response_delivery)
        log_ms.handle_log({'action': 'Delivery Assignment Store', 'status': storage_response_delivery.get('status'), 'details': f"Stored assignment for P-{parcel_id}"})
        
        if storage_response_delivery.get('status') != 'ACK':
             return {"status": "ERROR", "parcel_id": parcel_id, "message": "Failed to store final delivery assignment."}
             
        # 7. Controller-MS notifies Car_MS
        car_notify_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Car_MS",
            action='notify_delivery_assigned',
            payload={'parcel_id': parcel_id, 'car_id': car_id, 'details': temp_parcel_details}
        )
        
        # Car_MS acknowledges Controller_MS (This is car_notify_response)
        log_ms.handle_log({'action': 'Car Notification', 'status': car_notify_response.get('status'), 'details': f"Notified Car {car_id} for Parcel {parcel_id}"})
        
        # 8. Controller_MS notifies UI_MS
        ui_notify_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="UI_MS",
            action='notify_delivery_progress',
            payload={'parcel_id': parcel_id, 'status': 'ASSIGNED', 'message': f"Delivery assigned to Car {car_id}. Parcel ID: {parcel_id}"}
        )
        
        # UI_MS acknowledges Controller_MS (This is ui_notify_response)
        log_ms.handle_log({'action': 'UI Notification', 'status': ui_notify_response.get('status'), 'details': "UI/Sender notification completed."})
        
        return {"status": "SUCCESS", "parcel_id": parcel_id, "message": "Delivery process completed and assigned."}

    # --- Major Flow 2: Delivery Update ---
    def _handle_car_update_request(self, request_payload):
        """
        Handles the delivery update flow initiated by Car_MS.
        """
        log_ms = Log_MS()
        
        # Car_MS requests delivery update from Controller_MS (This call is the start)
        # Controller_MS acknowledges Car_MS (Handled by the dispatcher returning success)
        
        # Assume Car_MS sends the relevant parcel ID and new status
        # For simulation, we'll manually define the update action for the latest parcel
        
        # Find the latest assigned parcel ID
        latest_parcel_id = next(reversed(DATABASE_PARCEL_DATA.keys()), None)
        
        if not latest_parcel_id:
            log_ms.handle_log({'action': 'Update Request Failed', 'status': 'ERROR', 'details': "No parcels available for update."})
            return {"status": "ERROR", "message": "No active deliveries to update."}
            
        new_status = "DELIVERED" # Simulating a final status update
        
        # Controller_MS shares delivery update with Storage_MS
        storage_update_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="Storage_MS",
            action='update_delivery_status',
            payload={'parcel_id': latest_parcel_id, 'new_status': new_status}
        )
        
        # Storage_MS acknowledges Controller_MS (This is storage_update_response)
        log_ms.handle_log({'action': 'Delivery Update Store', 'status': storage_update_response.get('status'), 'details': f"Updated P-{latest_parcel_id} to {new_status}"})
        
        if storage_update_response.get('status') != 'ACK':
            return {"status": "ERROR", "message": "Failed to update delivery status in Storage_MS."}
        
        # Controller_MS notifies UI_MS
        ui_notify_response = simulate_yaml_exchange(
            sender_ms="Controller_MS",
            target_ms="UI_MS",
            action='notify_delivery_progress',
            payload={'parcel_id': latest_parcel_id, 'status': new_status, 'message': f"Delivery for Parcel {latest_parcel_id} is now {new_status}."}
        )
        
        # UI_MS acknowledges Controller_MS (This is ui_notify_response)
        log_ms.handle_log({'action': 'UI Notification (Update)', 'status': ui_notify_response.get('status'), 'details': "UI/Sender update notification completed."})

        return {"status": "SUCCESS", "message": f"Delivery update for {latest_parcel_id} processed."}


# --- Main Execution Simulation ---

if __name__ == "__main__":
    print("--- Starting Delivery Microservice System Simulation ---")
    
    # Initialize all services (registers their handlers)
    controller_instance = Controller_MS()
    sender_instance = Sender_MS()
    car_instance = Car_MS()

    # Define the initial delivery request payload (from Sender_MS perspective)
    test_delivery_details = {
        "destination": "123 Main St, Anytown",
        "recipient": "Jane Doe",
        "item_description": "Laptop charger",
        "priority": "HIGH"
    }

    # --- 1. Initiate Delivery Assignment Flow (The main sequence) ---
    print("\n\n#####################################################")
    print("## STAGE 1: DELIVERY ASSIGNMENT & FULL ORCHESTRATION ##")
    print("#####################################################")
    
    sender_instance.request_delivery(test_delivery_details)

    # --- 2. Initiate Delivery Update Flow (The final sequence) ---
    print("\n\n#####################################################")
    print("## STAGE 2: DELIVERY UPDATE (INITIATED BY CAR_MS) ##")
    print("#####################################################")
    
    # Simulate Car_MS initiating a status update after the successful assignment
    simulate_yaml_exchange(
        sender_ms="Car_MS",
        target_ms="Controller_MS",
        action='request_delivery_update',
        payload={'car_id': 'CAR-001', 'status_update': 'DELIVERED'}
    )

    # --- 3. Final State Summary ---
    print("\n\n#####################################################")
    print("## SYSTEM STATE SUMMARY ##")
    print("#####################################################")
    print("\n--- Database_1 (Parcel Data) ---")
    print(json.dumps(DATABASE_PARCEL_DATA, indent=2))
    
    print("\n--- Database_2 (Delivery Assignment) ---")
    print(json.dumps(DATABASE_DELIVERY_ASSIGNMENT, indent=2))
    
    print("\n--- Database_3 (Logs - Last 5 Entries) ---")
    print(json.dumps(DATABASE_LOGS[-5:], indent=2))
