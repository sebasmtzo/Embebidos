# Create a web app using Flask to visualize data stored in CSV files
from flask import Flask, render_template, jsonify
import pandas as pd
import os

app = Flask(__name__)

# --- File Constants ---
# Names of the CSV files used by the application
LOG_FILENAME = "parkly_log.csv"
COUNTS_FILENAME = "parkly_counts.csv"

# IMPORTANT: BASE DIRECTORY WHERE THE CSV FILES ARE STORED
# This must be the EXACT path to the directory that contains
# parkly_log.csv and parkly_counts.csv
BASE_PATH = '/home/jseba/embedded25b/proyecto/MQTT_Client/'

def load_csv(filename):
    """
    Helper function to load a CSV file and return a Pandas DataFrame.

    If the file does not exist, an empty DataFrame with the expected
    structure is returned in order to prevent runtime errors.
    """
    
    # Build the absolute path by combining the base directory and file name
    csv_path = os.path.join(BASE_PATH, filename) 
    
    if not os.path.exists(csv_path):
        # If the CSV file does not exist, return an empty DataFrame
        # with predefined columns depending on the requested file
        if filename == LOG_FILENAME:
            df = pd.DataFrame(columns=['timestamp', 'spot', 'status', 'battery'])
        elif filename == COUNTS_FILENAME:
            df = pd.DataFrame(columns=['timestamp', 'spot', 'event_type', 'total_count'])
        else:
            # If the filename is unknown, raise an explicit error
            raise FileNotFoundError(f"CSV file not found: {filename}")
        
        # This avoids read errors when the MQTT client has not written any data yet
        return df

    # Read the CSV file from the absolute path
    df = pd.read_csv(csv_path)
    return df

@app.route('/')
def index():
    """
    Main route that serves the HTML template for the web interface.
    """
    return render_template('index.html')

@app.route('/load-log', methods=['GET'])
def load_log():
    """
    Loads the detailed log file (parkly_log.csv) and returns its
    contents and metadata in JSON format.
    """
    try:
        df = load_csv(LOG_FILENAME)
        
        data = {
            'success': True,
            'columns': df.columns.tolist(),          # Column names
            'data': df.to_dict('list'),              # Full dataset as lists
            'shape': f'{df.shape[0]} rows × {df.shape[1]} columns',
            'filename': LOG_FILENAME,
            # HTML preview of the last 10 rows for display in the frontend
            'preview': df.tail(10).to_html(
                classes='table table-striped table-sm',
                index=False
            )
        }
        
        return jsonify(data)
    
    except pd.errors.ParserError:
        # Error when the CSV format is invalid or corrupted
        return jsonify({'error': 'Invalid CSV file format (Log)'}), 400
    except FileNotFoundError as e:
        # Error when the CSV file is missing
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        # Catch-all error handler for unexpected failures
        return jsonify({'error': str(e)}), 500

@app.route('/load-counts', methods=['GET'])
def load_counts():
    """
    Loads the usage count file (parkly_counts.csv) and prepares
    aggregated data for visualization (e.g., bar charts).
    """
    try:
        df = load_csv(COUNTS_FILENAME)
        
        # For the bar chart, group by parking spot and retrieve
        # the most recent total count for each spot
        if not df.empty:
            final_counts = df.groupby('spot')['total_count'].last().reset_index()
        else:
            # Default values when no data is available yet
            final_counts = pd.DataFrame({'spot': ['A', 'B'], 'total_count': [0, 0]})
        
        data = {
            'success': True,
            'counts_data': final_counts.to_dict('list'),
            # HTML preview of the last 10 rows for debugging/inspection
            'preview': df.tail(10).to_html(
                classes='table table-striped table-sm',
                index=False
            )
        }
        
        return jsonify(data)
    
    except pd.errors.ParserError:
        # Error when the CSV format is invalid or corrupted
        return jsonify({'error': 'Invalid CSV file format (Counts)'}), 400
    except FileNotFoundError as e:
        # Error when the CSV file is missing
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        # Catch-all error handler for unexpected failures
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # It is recommended to disable debug mode in production environments
    app.run(debug=True)
