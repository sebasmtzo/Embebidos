# MQTT Client para recibir datos JSON, guardarlos en CSV, y contar transiciones de uso (0 -> 1).
# Tópico: esp32/parkly

import paho.mqtt.client as mqtt
import json
import csv
from datetime import datetime

# --- Configuración del Archivo de Log Detallado (Datos Brutos) ---
LOG_FILENAME = "parkly_log.csv"
LOG_HEADERS = ['timestamp', 'spot', 'status', 'battery']

# --- Configuración del Archivo de Conteo (Transiciones 0->1) ---
COUNT_FILENAME = "parkly_counts.csv"
COUNT_HEADERS = ['timestamp', 'spot', 'event_type', 'total_count'] 

# --- Configuración del Broker MQTT ---
BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "esp32/parkly"

# --- Variables Globales para el Conteo y Estado ---
# Inicializamos el último estado conocido de cada spot (asumimos libres al inicio)
# Es crucial que esta variable se defina fuera de las función para persistir entre mensajes.
LAST_SPOT_STATUS = {
    'A': 0, 
    'B': 0
}
# Contadores de uso TOTALES
SPOT_USAGE_COUNT = {
    'A': 0,
    'B': 0
}
# La lista de todos los spots que estamos monitoreando
KNOWN_SPOTS = ['A', 'B']

def initialize_csv(filename, headers):
    """Crea un archivo CSV con los encabezados si no existe."""
    try:
        with open(filename, mode='x', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        print(f"Archivo CSV '{filename}' inicializado con encabezados.")
    except FileExistsError:
        pass

def on_connect(client, userdata, flags, rc):
    """Callback que se llama cuando el cliente recibe una respuesta de conexión del broker."""
    if rc == 0:
        print("¡Conectado a MQTT Broker!")
        client.subscribe(TOPIC)
        print(f"Suscrito al tópico: {TOPIC}")
    else:
        print(f"Fallo al conectar, código de retorno: {rc}")

def on_message(client, userdata, msg):
    """Callback que se llama cuando se recibe un mensaje publicado en el tópico suscrito."""
    # Las variables globales deben ser declaradas dentro de la función si se van a modificar
    global LAST_SPOT_STATUS, SPOT_USAGE_COUNT
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 1. Decodificar la carga útil a string
        payload_str = msg.payload.decode('utf-8')
        # 2. Parsear el string JSON a un diccionario de Python
        data = json.loads(payload_str)
        
        current_spot = data.get('spot')
        current_status = data.get('status')
        current_battery = data.get('battery')
        
        # Validación básica de datos
        if current_spot not in KNOWN_SPOTS or not isinstance(current_status, int):
             print(f"[{now}] ADVERTENCIA: Datos ignorados (Spot desconocido o status no es entero).")
             return

        # --- 3. Log Detallado (Guardar todos los datos brutos en parkly_log.csv) ---
        row_log_data = {
            'timestamp': now,
            'spot': current_spot,
            'status': current_status,
            'battery': current_battery
        }
        with open(LOG_FILENAME, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=LOG_HEADERS)
            writer.writerow(row_log_data)
        
        # --- 4. Lógica de Conteo de Transición 0 -> 1 ---
        
        # Obtener el estado anterior del spot
        last_status = LAST_SPOT_STATUS.get(current_spot)
        
        # 💡 Detectar la Transición: De LIBRE (0) a OCUPADO (1)
        if last_status == 0 and current_status == 1:
            SPOT_USAGE_COUNT[current_spot] += 1
            
            # Registrar el evento de conteo en el segundo archivo CSV (parkly_counts.csv)
            row_count_data = {
                'timestamp': now,
                'spot': current_spot,
                'event_type': 'occupied',
                'total_count': SPOT_USAGE_COUNT[current_spot] # Incluimos el contador total
            }
            with open(COUNT_FILENAME, mode='a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=COUNT_HEADERS)
                writer.writerow(row_count_data)
            
            print(f"\n*** ¡OCUPADO! (0->1) ***")
            print(f"Spot: {current_spot} | Nuevo Conteo Total: {SPOT_USAGE_COUNT[current_spot]}")
            print(f"Evento registrado en {COUNT_FILENAME}")
            
        # Actualizar el estado anterior con el estado actual para la próxima comparación
        LAST_SPOT_STATUS[current_spot] = current_status
        
        # print(f"[{now}] Spot: {current_spot}, Estado Actualizado: {current_status}. Log guardado.")

    except json.JSONDecodeError:
        print(f"[{now}] ERROR: Mensaje no es JSON válido: {msg.payload.decode()}")
    except KeyError as e:
        print(f"[{now}] ERROR: Clave JSON faltante: {e}. Payload: {payload_str}")
    except Exception as e:
        print(f"[{now}] ERROR Inesperado: {e}")

def main():
    # Inicializar ambos archivos CSV
    initialize_csv(LOG_FILENAME, LOG_HEADERS)
    initialize_csv(COUNT_FILENAME, COUNT_HEADERS)

    print("--- Configuración Inicial ---")
    print(f"Spots iniciales (último estado): {LAST_SPOT_STATUS}")
    print(f"Contadores iniciales (uso total): {SPOT_USAGE_COUNT}")
    print("----------------------------")
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Conectando a broker {BROKER}:{PORT}...")
    client.connect(BROKER, PORT, 60)

    # Bloqueo para procesar tráfico de red y despachar callbacks
    client.loop_forever()

if __name__ == "__main__":
    main()