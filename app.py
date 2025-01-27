import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sklearn.ensemble import IsolationForest
from flask import Flask

# Initialize Flask app for web dyno
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Environment configuration
DATABASE_URL = os.environ.get("DATABASE_URL", "").replace("postgres://", "postgresql://")
BLACKLIST_API_URL = os.environ.get("BLACKLIST_API_URL", "https://api.gopluslabs.io/api/v1/token_security/1")
SLEEP_INTERVAL = int(os.environ.get("SLEEP_INTERVAL", "300"))  # 5 minute default

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
        """Initialize database schema"""
        with self.engine.connect() as conn:
            try:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS blacklist (
                        address VARCHAR(42) PRIMARY KEY,
                        type VARCHAR(20) CHECK (type IN ('coin', 'dev')),
                        reason TEXT,
                        listed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_blacklist_type ON blacklist(type);
                """))
                logging.info("Database schema verified")
            except SQLAlchemyError as e:
                logging.error(f"Database initialization failed: {e}")
                raise

    def refresh_blacklist(self):
        """Update blacklist from external API"""
        try:
            response = requests.get(BLACKLIST_API_URL, timeout=10)
            response.raise_for_status()
            
            for token in response.json().get("tokens", []):
                if token.get("is_honeypot", False):
                    self._update_blacklist_entry(
                        token["contract_address"],
                        "coin",
                        "Automated honeypot detection"
                    )
            logging.info("Blacklist updated successfully")
            
        except Exception as e:
            logging.error(f"Blacklist update failed: {e}")

    def _update_blacklist_entry(self, address: str, list_type: str, reason: str):
        """Upsert blacklist entry"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO blacklist (address, type, reason)
                    VALUES (:address, :type, :reason)
                    ON CONFLICT (address) DO UPDATE
                    SET reason = EXCLUDED.reason
                """), {"address": address, "type": list_type, "reason": reason})
                
        except SQLAlchemyError as e:
            logging.error(f"Blacklist update error for {address}: {e}")

    def run_monitor(self):
        """Main monitoring loop"""
        while True:
            try:
                self.refresh_blacklist()
                time.sleep(SLEEP_INTERVAL)
            except Exception as e:
                logging.error(f"Monitoring error: {e}")
                time.sleep(60)

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
    import threading
    start_background_task()
    
    # Start Flask web server
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        use_reloader=False
    )