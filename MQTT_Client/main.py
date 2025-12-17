# MQTT client to receive JSON data, store it into CSV files,
# and count usage transitions (0 -> 1).
# Topic: esp32/parkly

import paho.mqtt.client as mqtt
import json
import csv
from datetime import datetime

# --- Detailed Log File Configuration (Raw Data) ---
# This CSV stores every received message without filtering
LOG_FILENAME = "parkly_log.csv"
LOG_HEADERS = ['timestamp', 'spot', 'status', 'battery']

# --- Usage Count File Configuration (0 -> 1 Transitions) ---
# This CSV stores only occupancy events and cumulative counts
COUNT_FILENAME = "parkly_counts.csv"
COUNT_HEADERS = ['timestamp', 'spot', 'event_type', 'total_count'] 

# --- MQTT Broker Configuration ---
BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "esp32/parkly"

# --- Global Variables for State Tracking and Counting ---
# Initialize the last known state of each parking spot
# Assumption: all spots start as free (0)
# IMPORTANT: These variables must be defined globally
# so their values persist across multiple MQTT messages.
LAST_SPOT_STATUS = {
    'A': 0, 
    'B': 0
}

# Total usage counters per parking spot
SPOT_USAGE_COUNT = {
    'A': 0,
    'B': 0
}

# List of all parking spots that the system recognizes
KNOWN_SPOTS = ['A', 'B']

def initialize_csv(filename, headers):
    """
    Creates a CSV file with headers if it does not already exist.
    This prevents runtime errors when appending new rows.
    """
    try:
        with open(filename, mode='x', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        print(f"CSV file '{filename}' initialized with headers.")
    except FileExistsError:
        # If the file already exists, no action is needed
        pass

def on_connect(client, userdata, flags, rc):
    """
    Callback executed when the MQTT client receives a connection
    response from the broker.
    """
    if rc == 0:
        print("Successfully connected to MQTT broker!")
        client.subscribe(TOPIC)
        print(f"Subscribed to topic: {TOPIC}")
    else:
        print(f"Failed to connect, return code: {rc}")

def on_message(client, userdata, msg):
    """
    Callback executed whenever a message is received on the
    subscribed MQTT topic.
    """
    # Declare global variables that will be modified inside this function
    global LAST_SPOT_STATUS, SPOT_USAGE_COUNT
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 1. Decode the message payload from bytes to string
        payload_str = msg.payload.decode('utf-8')

        # 2. Parse the JSON string into a Python dictionary
        data = json.loads(payload_str)
        
        current_spot = data.get('spot')
        current_status = data.get('status')
        current_battery = data.get('battery')
        
        # Basic data validation
        if current_spot not in KNOWN_SPOTS or not isinstance(current_status, int):
             print(f"[{now}] WARNING: Ignored data (Unknown spot or status is not an integer).")
             return

        # --- 3. Detailed Logging (Store all raw data in parkly_log.csv) ---
        row_log_data = {
            'timestamp': now,
            'spot': current_spot,
            'status': current_status,
            'battery': current_battery
        }
        with open(LOG_FILENAME, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=LOG_HEADERS)
            writer.writerow(row_log_data)
        
        # --- 4. Transition Counting Logic (0 -> 1) ---
        
        # Retrieve the previous state of the current parking spot
        last_status = LAST_SPOT_STATUS.get(current_spot)
        
        # 💡 Detect transition: FREE (0) -> OCCUPIED (1)
        if last_status == 0 and current_status == 1:
            SPOT_USAGE_COUNT[current_spot] += 1
            
            # Log the occupancy event in the second CSV file (parkly_counts.csv)
            row_count_data = {
                'timestamp': now,
                'spot': current_spot,
                'event_type': 'occupied',
                'total_count': SPOT_USAGE_COUNT[current_spot]  # Cumulative counter
            }
            with open(COUNT_FILENAME, mode='a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=COUNT_HEADERS)
                writer.writerow(row_count_data)
            
            print(f"\n*** OCCUPIED! (0->1) ***")
            print(f"Spot: {current_spot} | New Total Count: {SPOT_USAGE_COUNT[current_spot]}")
            print(f"Event stored in {COUNT_FILENAME}")
            
        # Update the last known state for the next comparison
        LAST_SPOT_STATUS[current_spot] = current_status
        
        # print(f"[{now}] Spot: {current_spot}, State updated: {current_status}. Log saved.")

    except json.JSONDecodeError:
        print(f"[{now}] ERROR: Message is not valid JSON: {msg.payload.decode()}")
    except KeyError as e:
        print(f"[{now}] ERROR: Missing JSON key: {e}. Payload: {payload_str}")
    except Exception as e:
        print(f"[{now}] Unexpected ERROR: {e}")

def main():
    """
    Main entry point of the MQTT client application.
    Initializes CSV files, configures MQTT callbacks,
    and starts the blocking network loop.
    """
    # Initialize both CSV files if they do not exist
    initialize_csv(LOG_FILENAME, LOG_HEADERS)
    initialize_csv(COUNT_FILENAME, COUNT_HEADERS)

    print("--- Initial Configuration ---")
    print(f"Initial spots (last known state): {LAST_SPOT_STATUS}")
    print(f"Initial counters (total usage): {SPOT_USAGE_COUNT}")
    print("-----------------------------")
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Connecting to broker {BROKER}:{PORT}...")
    client.connect(BROKER, PORT, 60)

    # Blocking call to process network traffic and dispatch callbacks
    client.loop_forever()

if __name__ == "__main__":
    main()
