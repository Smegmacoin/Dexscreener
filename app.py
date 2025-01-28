import os
import time
import logging
import threading
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sklearn.ensemble import IsolationForest
from flask import Flask

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

    # Rest of DexMonitor class remains the same as your original code...

# Web endpoint for health checks
@app.route('/')
def health_check():
    return "DEX Monitor Operational - UTC: " + datetime.utcnow().isoformat()

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