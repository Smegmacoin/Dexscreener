import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sklearn.ensemble import IsolationForest

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
DATABASE_URL = os.environ.get("DATABASE_URL")  # Heroku provides this
BLACKLIST_API_URL = os.environ.get("BLACKLIST_API_URL", "https://api.gopluslabs.io/api/v1/token_security/1")
SLEEP_INTERVAL = int(os.environ.get("SLEEP_INTERVAL", "100"))  # Default: 100 seconds

class EnhancedDexScreenerBot:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
        self._init_db()
        self.model = IsolationForest(n_estimators=100, contamination=0.01)
        self.historical_data = self._load_historical_data()

    def _init_db(self):
        """Initialize database tables."""
        try:
            with self.engine.connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS blackList (
                        address VARCHAR(42) PRIMARY KEY,
                        type VARCHAR(20) CHECK (type IN ('coin', 'dev')),
                        reason TEXT,
                        listed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE INDEX IF NOT EXISTS idx_blacklist_type ON blackList(type);
                """)
                logging.info("Database initialized successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Database initialization failed: {e}")

    def _load_historical_data(self):
        """Load historical data for anomaly detection."""
        try:
            return pd.read_sql("SELECT * FROM pairs", self.engine)
        except Exception as e:
            logging.error(f"Failed to load historical data: {e}")
            return pd.DataFrame()

    def _refresh_blackLists(self):
        """Refresh blacklists from external sources."""
        try:
            response = requests.get(BLACKLIST_API_URL)
            response.raise_for_status()
            data = response.json()

            for token in data.get("tokens", []):
                if token.get("is_honeypot", False):
                    self.add_to_blackList(
                        token["contract_address"],
                        "coin",
                        "Automated honeypot detection"
                    )
            logging.info("Blacklist refreshed successfully.")
        except Exception as e:
            logging.error(f"Blacklist refresh failed: {e}")

    def add_to_blackList(self, address: str, list_type: str, reason: str):
        """Add an entry to the blacklist."""
        try:
            with self.engine.connect() as conn:
                conn.execute("""
                    INSERT INTO blackList (address, type, reason)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (address) DO UPDATE
                    SET reason = EXCLUDED.reason
                """, (address, list_type, reason))
                logging.info(f"Added {address} to blacklist.")
        except SQLAlchemyError as e:
            logging.error(f"Failed to add to blacklist: {e}")

    def run(self):
        """Main loop for fetching, processing, and analyzing data."""
        while True:
            try:
                self._refresh_blackLists()
                time.sleep(SLEEP_INTERVAL)
            except Exception as e:
                logging.error(f"Runtime error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    bot = EnhancedDexScreenerBot()
    bot.run()
