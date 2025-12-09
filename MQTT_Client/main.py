# MQTT Client to fetch data from the broker with topic esp32/heartbeat

import paho.mqtt.client as mqtt

# MQTT broker settings
BROKER = "test.mosquitto.org"  # You can change this to your broker
PORT = 1883
TOPIC = "esp32/heartbeat"

def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected to MQTT Broker!")
		client.subscribe(TOPIC)
	else:
		print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
	#print(f"Received message from {msg.topic}: {msg.payload.decode()}")
	
    # read message and convert to integer
    try:
        heartbeat = int(msg.payload.decode())
        print(f"{heartbeat}")
    except ValueError:
        print("Received non-integer heartbeat value")
		
    # Append to a file starting with date and time separated by comma
    from datetime import datetime
    with open("heartbeat_log.csv", "a") as f:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{now},{heartbeat}\n")
    

def main():
	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message

	print(f"Connecting to broker {BROKER}:{PORT}...")
	client.connect(BROKER, PORT, 60)

	# Blocking loop to process network traffic and dispatch callbacks
	client.loop_forever()

if __name__ == "__main__":
	main()