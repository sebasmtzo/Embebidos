# Create a web app using flask to plot data from my .csv file
from flask import Flask, render_template, jsonify
import pandas as pd
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/load-default', methods=['GET'])
def load_default():
    """Load the default CSV file and return data as JSON"""
    try:
        csv_path = '/home/jseba/embedded25b/proyecto/MQTT_Client/heartbeat_log.csv'
        if not os.path.exists(csv_path):
            return jsonify({'error': f'CSV file not found at {csv_path}'}), 404
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Get column names
        columns = df.columns.tolist()
        
        # Convert dataframe to JSON-serializable format
        data = {
            'success': True,
            'columns': columns,
            'data': df.to_dict('list'),
            'shape': f'{df.shape[0]} rows × {df.shape[1]} columns',
            'filename': os.path.basename(csv_path),
            'preview': df.head(10).to_html(classes='table table-striped')
        }
        
        return jsonify(data)
    
    except pd.errors.ParserError:
        return jsonify({'error': 'Invalid CSV file'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)