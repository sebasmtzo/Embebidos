# MQTT Client (esp32/heartbeat)

Simple MQTT subscriber that listens to the `esp32/heartbeat` topic and prints incoming messages.

Requirements

- Python 3.8+
- Install dependencies:

```bash
pip install -r requirements.txt
```

Usage

- Run with defaults (broker localhost:1883, topic `esp32/heartbeat`):

```bash
python3 main.py
```

- Specify broker/port/topic via environment variables or CLI:

```bash
MQTT_BROKER=192.168.1.50 MQTT_PORT=1883 python3 main.py
# or
python3 main.py --broker 192.168.1.50 --port 1883 --topic esp32/heartbeat
```

Notes

- Payloads that are valid JSON are pretty-printed.
- Ctrl-C will disconnect cleanly.
