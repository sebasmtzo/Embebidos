# Create a web app using flask to plot data from my .csv file
from flask import Flask, render_template, jsonify
import pandas as pd
import os

app = Flask(__name__)

# --- Constantes de Archivos ---
LOG_FILENAME = "parkly_log.csv"
COUNTS_FILENAME = "parkly_counts.csv"

# IMPORTANTE: RUTA BASE DONDE ESTÁN LOS CSVs
# Esta debe ser la dirección EXACTA del directorio que contiene parkly_log.csv y parkly_counts.csv
BASE_PATH = '/home/jseba/embedded25b/proyecto/MQTT_Client/'

def load_csv(filename):
    """Función auxiliar para cargar un archivo CSV y retornar el DataFrame."""
    
    # Construir la ruta completa combinando la ruta base con el nombre del archivo
    csv_path = os.path.join(BASE_PATH, filename) 
    
    if not os.path.exists(csv_path):
        # Si el archivo no existe en la ruta absoluta, se retorna un DataFrame vacío.
        if filename == LOG_FILENAME:
            df = pd.DataFrame(columns=['timestamp', 'spot', 'status', 'battery'])
        elif filename == COUNTS_FILENAME:
            df = pd.DataFrame(columns=['timestamp', 'spot', 'event_type', 'total_count'])
        else:
             # Si no es uno de los archivos conocidos, se levanta el error
             raise FileNotFoundError(f"CSV file not found: {filename}")
        
        # Esto previene errores de lectura si el cliente MQTT aún no ha escrito nada
        return df

    # Leemos el CSV desde la ruta absoluta
    df = pd.read_csv(csv_path)
    return df

@app.route('/')
def index():
    """Ruta principal que sirve la plantilla HTML."""
    return render_template('index.html')

@app.route('/load-log', methods=['GET'])
def load_log():
    """Carga el archivo de log detallado (parkly_log.csv)."""
    try:
        df = load_csv(LOG_FILENAME)
        
        data = {
            'success': True,
            'columns': df.columns.tolist(),
            'data': df.to_dict('list'),
            'shape': f'{df.shape[0]} rows × {df.shape[1]} columns',
            'filename': LOG_FILENAME,
            'preview': df.tail(10).to_html(classes='table table-striped table-sm', index=False)
        }
        
        return jsonify(data)
    
    except pd.errors.ParserError:
        return jsonify({'error': 'Invalid CSV file format (Log)'}), 400
    except FileNotFoundError as e:
         return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/load-counts', methods=['GET'])
def load_counts():
    """Carga el archivo de conteo de uso (parkly_counts.csv)."""
    try:
        df = load_csv(COUNTS_FILENAME)
        
        # Para la gráfica de barras, agrupamos por spot y obtenemos el conteo total más reciente
        if not df.empty:
            final_counts = df.groupby('spot')['total_count'].last().reset_index()
        else:
            final_counts = pd.DataFrame({'spot': ['A', 'B'], 'total_count': [0, 0]})
        
        data = {
            'success': True,
            'counts_data': final_counts.to_dict('list'),
            'preview': df.tail(10).to_html(classes='table table-striped table-sm', index=False)
        }
        
        return jsonify(data)
    
    except pd.errors.ParserError:
        return jsonify({'error': 'Invalid CSV file format (Counts)'}), 400
    except FileNotFoundError as e:
         return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Es recomendable desactivar debug en producción
    app.run(debug=True)