import uuid
import time
import json # Using JSON for simplicity, but the structure simulates YAML data

# --- Mock Databases (Internal to the System) ---
DATABASE_1_PARCELS = {} # Stores parcel data (Database_1)
DATABASE_2_ASSIGNMENTS = {} # Stores delivery assignments (Database_2)
DATABASE_3_LOGS = [] # Stores logs (Database_3)

# --- Service Mockup Functions ---

def simulate_yaml_communication(sender_name: str, receiver_name: str, payload: dict) -> dict:
    """Simulates the serialization (to YAML) and deserialization of a request/response."""
    
    # In a real microservice environment, the 'payload' would be converted to a 
    # YAML string, sent over a network, and then parsed back into a dictionary 
    # by the receiver. This function simulates that data exchange.
    
    print(f"\n[COMM] {sender_name} -> {receiver_name}: Request '{payload.get('action')}' with data: {json.dumps(payload.get('data', {}))}")
    time.sleep(0.01) # Simulate network delay
    
    # This is a placeholder for the actual request handling logic within the receiver function.
    # For this simulation, the controller calls the actual MS functions directly.
    return {"status": "ACK", "data": f"Response received by {sender_name}"}

def Log_MS(log_data: dict) -> dict:
    """Microservice responsible for logging operations (Database_3)."""
    # Log_MS stores logs in Database_3
    log_entry = {
        "timestamp": time.time(),
        "source": log_data.get('source'),
        "message": log_data.get('message')
    }
    DATABASE_3_LOGS.append(log_entry)
    print(f"[Log_MS] Logged event from {log_entry['source']}")
    return {"status": "success"}

def Storage_MS(request: dict) -> dict:
    """Microservice handling Database_1 (Parcels) and Database_2 (Assignments) access."""
    action = request.get("action")
    data = request.get("data", {})
    response_data = {}

    if action == "store_parcel_id":
        # IDGen_MS shares parcel ID with Storage_MS
        # Storage_MS stores parcel ID in Database_2
        parcel_id = data["parcel_id"]
        DATABASE_2_ASSIGNMENTS[parcel_id] = {"status": "ID_GENERATED"}
        print(f"[Storage_MS] Stored Parcel ID {parcel_id} in Database_2.")
        response_data = {"message": "Parcel ID stored"}
    
    elif action == "store_car_id":
        # Car_MS shares car ID with Storage_MS
        # Storage_MS stores car ID in Database_2
        parcel_id = data["parcel_id"]
        car_id = data["car_id"]
        if parcel_id in DATABASE_2_ASSIGNMENTS:
            DATABASE_2_ASSIGNMENTS[parcel_id]["car_id"] = car_id
            DATABASE_2_ASSIGNMENTS[parcel_id]["status"] = "CAR_ASSIGNED"
            print(f"[Storage_MS] Stored Car ID {car_id} for {parcel_id} in Database_2.")
            response_data = {"message": "Car ID stored"}
        else:
             response_data = {"error": "Parcel ID not found"}

    elif action == "get_parcel_id":
        # Controller_MS requests parcel ID from Storage_MS
        parcel_id = data["parcel_id"]
        response_data = {"parcel_id": parcel_id}
        print(f"[Storage_MS] Retrieved Parcel ID {parcel_id}.")

    elif action == "get_car_id":
        # Controller_MS requests car ID from Storage_MS
        parcel_id = data["parcel_id"]
        car_id = DATABASE_2_ASSIGNMENTS.get(parcel_id, {}).get("car_id")
        response_data = {"car_id": car_id}
        print(f"[Storage_MS] Retrieved Car ID {car_id}.")

    elif action == "store_delivery":
        # Controller_MS shares delivery with Storage_MS
        # Storage_MS stores delivery in Database_1 (Parcel Data)
        delivery_data = data["delivery"]
        parcel_id = delivery_data["parcel_id"]
        DATABASE_1_PARCELS[parcel_id] = delivery_data
        print(f"[Storage_MS] Stored complete delivery data for {parcel_id} in Database_1.")
        response_data = {"message": "Delivery assignment stored in DB1"}

    elif action == "update_delivery_status":
        # Controller_MS shares delivery update with Storage_MS
        # Storage_MS updates delivery in Database_1
        parcel_id = data["parcel_id"]
        new_status = data["status"]
        if parcel_id in DATABASE_1_PARCELS:
            DATABASE_1_PARCELS[parcel_id]["delivery_status"] = new_status
            print(f"[Storage_MS] Updated delivery status for {parcel_id} to {new_status} in Database_1.")
            response_data = {"message": "Delivery status updated"}
        else:
             response_data = {"error": "Parcel ID not found"}

    return {"status": "ACK", "data": response_data}

def IDGen_MS(request: dict) -> dict:
    """Microservice responsible for generating unique parcel IDs."""
    
    # IDGen_MS generates parcel ID
    parcel_id = str(uuid.uuid4())[:8]
    
    # IDGen_MS shares parcel ID with Storage_MS
    simulate_yaml_communication("IDGen_MS", "Storage_MS", {
        "action": "store_parcel_id", 
        "data": {"parcel_id": parcel_id}
    })
    
    # Storage_MS acknowledges IDGen_MS (Simulated via return status check)
    # The Storage_MS call above is assumed successful.
    print(f"[IDGen_MS] Received ACK from Storage_MS.")
    
    # IDGen_MS acknowledges Controller_MS
    return {"status": "ACK", "data": {"parcel_id": parcel_id}}

def Car_MS(request: dict) -> dict:
    """External Microservice managing car logistics (Laptop_1, Windows)."""
    action = request.get("action")
    data = request.get("data", {})
    
    if action == "request_car_id":
        # Car_MS checks car ID (Simulated check)
        car_id = f"CAR-{time.strftime('%H%M%S')}"
        print(f"[Car_MS] Car ID checked and available: {car_id}")
        
        # Car_MS shares car ID with Storage_MS (via Controller_MS in the sequence, 
        # but here we simulate the data to be returned to the caller, Controller_MS)
        return {"status": "ACK", "data": {"car_id": car_id}}

    elif action == "notify_delivery_assigned":
        # Controller-MS notifies Car_MS
        print(f"[Car_MS] Received delivery assignment notification for {data.get('parcel_id')}.")
        # Car_MS acknowledges Controller_MS
        return {"status": "ACK"}

    elif action == "request_delivery_update":
        # Car_MS requests delivery update from Controller_MS (Simulated)
        parcel_id = data["parcel_id"]
        new_status = "DELIVERY_IN_TRANSIT"
        print(f"[Car_MS] Requesting status update for {parcel_id} to {new_status}.")
        return {"status": "ACK", "data": {"parcel_id": parcel_id, "status": new_status}}


def Sender_MS(request: dict) -> dict:
    """External Microservice initiating the request (Laptop_1, Windows)."""
    action = request.get("action")
    
    if action == "request_delivery":
        # Sender_MS requests delivery from UI_MS
        simulate_yaml_communication("Sender_MS", "UI_MS", request)
        print("[Sender_MS] Waiting for UI_MS response...")
        return {"status": "pending"}

    elif action == "delivery_notification":
        # UI_MS notifies Sender_MS
        print(f"[Sender_MS] Received final notification: {request['data']['message']}")
        # Sender_MS acknowledges UI_MS
        return {"status": "ACK"}


def UI_MS(request: dict) -> dict:
    """Internal Microservice acting as the gateway (Ubuntu, Server_1)."""
    action = request.get("action")
    
    if action == "request_delivery":
        print("[UI_MS] Received delivery request.")
        # UI_MS forwards 'request delivery' to Controller_MS
        response = Controller_MS(request)
        
        # UI_MS acknowledges Controller_MS (implicit upon returning the response)
        return response

    elif action == "notify_sender":
        # Controller_MS notifies UI_MS
        print("[UI_MS] Received delivery notification from Controller_MS.")
        
        # UI_MS notifies Sender_MS
        simulate_yaml_communication("UI_MS", "Sender_MS", {"action": "delivery_notification", "data": request["data"]})
        
        # Sender_MS acknowledges UI_MS (Simulated successful call above)
        print("[UI_MS] Received ACK from Sender_MS.")
        
        # UI_MS acknowledges Controller_MS
        return {"status": "ACK", "data": {"message": "Notification passed to Sender_MS"}}

def Controller_MS(request: dict) -> dict:
    """Internal Microservice: The central orchestrator (Ubuntu, Server_1)."""
    
    print("\n--- CONTROLLER_MS: START DELIVERY REQUEST ORCHESTRATION ---")
    parcel_details = request.get("data", {"origin": "A", "destination": "B"})
    
    current_parcel_id = None
    current_car_id = None
    
    # 1. Get Parcel ID
    # Controller_MS requests parcel ID from IDGen_MS
    print("\n[Controller_MS] Step 1: Requesting Parcel ID.")
    id_response = IDGen_MS({"action": "generate_id", "data": parcel_details})
    
    current_parcel_id = id_response["data"]["parcel_id"]
    # IDGen_MS acknowledges Controller_MS
    print(f"[Controller_MS] Received ACK from IDGen_MS. Parcel ID: {current_parcel_id}")

    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": f"Parcel ID {current_parcel_id} generated."})
    
    # 2. Get Car ID
    # Controller_MS requests car ID from Car_MS
    print("\n[Controller_MS] Step 2: Requesting Car ID.")
    car_request_response = Car_MS({"action": "request_car_id", "data": {"parcel_id": current_parcel_id}})
    
    current_car_id = car_request_response["data"]["car_id"]
    
    # Car_MS shares car ID with Storage_MS (via Controller in this flow)
    simulate_yaml_communication("Controller_MS", "Storage_MS", {
        "action": "store_car_id",
        "data": {"parcel_id": current_parcel_id, "car_id": current_car_id}
    })
    
    # Storage_MS acknowledges Car_MS (via Controller/implied)
    # Car_MS acknowledges Controller_MS (already handled by car_request_response)
    print(f"[Controller_MS] Received ACK from Car_MS. Car ID: {current_car_id}")
    
    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": f"Car ID {current_car_id} assigned to {current_parcel_id}."})
    
    # 3. Finalize and Assign Delivery
    
    # Controller_MS requests parcel ID from Storage_MS
    simulate_yaml_communication("Controller_MS", "Storage_MS", {
        "action": "get_parcel_id", "data": {"parcel_id": current_parcel_id}
    })
    
    # Controller_MS requests car ID from Storage_MS
    simulate_yaml_communication("Controller_MS", "Storage_MS", {
        "action": "get_car_id", "data": {"parcel_id": current_parcel_id}
    })
    
    # Controller_MS assigns delivery
    delivery_assignment = {
        "parcel_id": current_parcel_id,
        "car_id": current_car_id,
        "origin": parcel_details["origin"],
        "destination": parcel_details["destination"],
        "delivery_status": "ASSIGNED"
    }
    print(f"[Controller_MS] Delivery assigned: Car {current_car_id} takes {current_parcel_id}.")
    
    # Controller_MS shares delivery with Storage_MS
    storage_response = Storage_MS({"action": "store_delivery", "data": {"delivery": delivery_assignment}})
    # Storage_MS acknowledges Controller_MS (storage_response check)
    
    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": "Delivery assignment complete and stored."})
    
    # 4. Notify Services
    
    # Controller-MS notifies Car_MS
    car_notify_response = Car_MS({"action": "notify_delivery_assigned", "data": {"parcel_id": current_parcel_id}})
    # Car_MS acknowledges Controller_MS (car_notify_response check)
    
    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": "Car_MS notified of assignment."})

    # Controller_MS notifies UI_MS
    ui_notify_response = UI_MS({"action": "notify_sender", "data": {"parcel_id": current_parcel_id, "message": "Delivery assigned and scheduled."}})
    
    # UI_MS acknowledges Controller_MS (ui_notify_response check)
    
    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": "UI_MS and Sender_MS notified."})

    print("--- CONTROLLER_MS: DELIVERY ASSIGNMENT COMPLETE ---")
    
    # --- Simulated Delivery Update Flow (Second Part of Description) ---
    print("\n\n--- CONTROLLER_MS: START DELIVERY UPDATE FLOW ---")
    
    # Car_MS requests delivery update from Controller_MS
    update_request = Car_MS({"action": "request_delivery_update", "data": {"parcel_id": current_parcel_id}})
    update_data = update_request.get("data", {})

    # Controller_MS acknowledges Car_MS (implicit, already happened via update_request)
    print("[Controller_MS] Received delivery update request from Car_MS.")
    
    # Controller_MS shares delivery update with Storage_MS
    storage_update_response = Storage_MS({
        "action": "update_delivery_status",
        "data": {"parcel_id": update_data["parcel_id"], "status": update_data["status"]}
    })
    
    # Storage_MS acknowledges Controller_MS (storage_update_response check)
    
    # Controller_MS notifies UI_MS
    UI_MS({"action": "notify_sender", "data": {"parcel_id": current_parcel_id, "message": f"Delivery status updated to: {update_data['status']}"}})
    
    # UI_MS acknowledges Controller_MS (implicit in UI_MS function return)
    
    # Controller_MS shares logs with Log_MS
    Log_MS({"source": "Controller_MS", "message": f"Delivery status updated and UI_MS notified."})
    
    print("--- CONTROLLER_MS: DELIVERY UPDATE COMPLETE ---")
    
    return {"status": "success", "data": {"parcel_id": current_parcel_id}}


# --- Main Execution Flow ---
if __name__ == "__main__":
    print("Starting Delivery Microservice Simulation...")
    
    # Sender_MS initiates the process
    Sender_MS({"action": "request_delivery", "data": {"item": "Widgets", "recipient": "Alice"}})
    
    print("\n\n--- FINAL STATE CHECK ---")
    print(f"Database 1 (Parcel Data): {DATABASE_1_PARCELS}")
    print(f"Database 2 (Assignments): {DATABASE_2_ASSIGNMENTS}")
    print(f"Database 3 (Logs Count): {len(DATABASE_3_LOGS)} total logs generated.")
    
    # You can view the logs if needed, but they are verbose:
    # print("\n--- Sample Logs ---")
    # for log in DATABASE_3_LOGS[:5]:
    #     print(f"  [{log['source']}] {log['message']}")
