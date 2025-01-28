import os
import time
import logging
import threading
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sklearn.ensemble import IsolationForest
from flask import Flask, render_template_string

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Environment configuration (Heroku will provide DATABASE_URL)
DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgres://u6nc4l97ds0u0b:p246d69f559a0af43f9a277e314127325b16e50d2d0c618b2e6670b50502f2ef5@c2ihhcf1divl18.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d8uirubbaqeds3"
).replace("postgres://", "postgresql://")

BLACKLIST_API_URL = os.environ.get("BLACKLIST_API_URL", "https://api.gopluslabs.io/api/v1/token_security/1")
SLEEP_INTERVAL = int(os.environ.get("SLEEP_INTERVAL", "300"))  # 5 minutes default

class DexMonitor:
    def __init__(self):
        self.engine = self._create_db_engine()
        self._init_database()
        self.model = IsolationForest(n_estimators=100, contamination=0.01)
        self.historical_data = pd.DataFrame()

    def _create_db_engine(self):
        """Create database engine with connection pooling and retries"""
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                return create_engine(
                    DATABASE_URL,
                    pool_size=10,
                    max_overflow=20,
                    pool_recycle=300,
                    echo=False
                )
            except OperationalError as e:
                retries += 1
                logging.warning(f"DB connection failed (attempt {retries}/{max_retries}): {e}")
                time.sleep(10)
        raise RuntimeError("Could not establish database connection")

    def _init_database(self):
        """Initialize the database schema if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            token_name VARCHAR(255),
            volume FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_query))
                logging.info("Trades table created or already exists.")
        except Exception as e:
            logging.error(f"Error initializing database: {e}")

# Web endpoint for health checks
@app.route('/')
def health_check():
    return "DEX Monitor Operational - UTC: " + datetime.utcnow().isoformat()

# Web endpoint to display data in an HTML table
@app.route('/data', methods=['GET'])
def view_data():
    """Endpoint to display data in an HTML table"""
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Query the table (replace `trades` with your actual table name)
            result = conn.execute(text("SELECT * FROM trades LIMIT 50"))
            data = [dict(row) for row in result]

        # If no data is found
        if not data:
            return "<h1>No data available in the database.</h1>"

        # Render data in an HTML table
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Viewer</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { border-collapse: collapse; width: 100%; margin-top: 20px; }
                th, td { border: 1px solid black; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                h1 { color: #333; }
            </style>
        </head>
        <body>
            <h1>Data Viewer</h1>
            <table>
                <thead>
                    <tr>
                        {% for key in data[0].keys() %}
                        <th>{{ key }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for row in data %}
                    <tr>
                        {% for value in row.values() %}
                        <td>{{ value }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </body>
        </html>
        """
        return render_template_string(html_template, data=data)

    except Exception as e:
        # Handle errors
        logging.error(f"Error displaying data: {e}")
        return f"<h1>Error: {e}</h1>", 500

def start_background_task():
    """Start monitoring in background thread"""
    monitor = DexMonitor()
    monitor_thread = threading.Thread(target=monitor.run_monitor)
    monitor_thread.daemon = True
    monitor_thread.start()

if __name__ == "__main__":
    start_background_task()
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        use_reloader=False
    )