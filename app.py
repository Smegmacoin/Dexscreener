import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from flask import Flask, render_template_string
import logging

# Initialize Flask app
app = Flask(__name__)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database configuration
DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgresql://admin:your_password@localhost:5432/dexscreener"
)

# Filters
FILTERS = {
    "min_liquidity": 5000,  # USD
    "min_age_days": 3,
    "coin_blacklist": [
        "0x123...def",  # Known scam token address
    ]
}

# Create table if not exists
def initialize_database():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        token_address VARCHAR(255),
        liquidity FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        logging.info("Connecting to the database...")
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            logging.info("Trades table created successfully.")
    except Exception as e:
        logging.error(f"Error initializing database: {e}")

# Fetch trades from API (dummy for now)
@app.route('/fetch_trades')
def fetch_trades():
    try:
        # Simulate fetching trades from an external API
        trades = [
            {"token_address": "0xabc123...", "liquidity": 10000.0},
            {"token_address": "0xdef456...", "liquidity": 3000.0}
        ]
        # Filter trades
        filtered_trades = [
            trade for trade in trades
            if trade["liquidity"] >= FILTERS["min_liquidity"]
            and trade["token_address"] not in FILTERS["coin_blacklist"]
        ]
        # Insert into database
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            for trade in filtered_trades:
                conn.execute(text(
                    "INSERT INTO trades (token_address, liquidity) VALUES (:token_address, :liquidity)"
                ), trade)
        return "<h1>Trades fetched and saved successfully!</h1>"
    except Exception as e:
        logging.error(f"Error fetching trades: {e}")
        return f"<h1>Error: {e}</h1>", 500

# View trades in a table
@app.route('/view_trades')
def view_trades():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM trades"))
            trades = [dict(row) for row in result]
        if not trades:
            return "<h1>No trades found.</h1>"
        # Render trades in a table
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Trades</title>
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid black; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h1>Trades</h1>
            <table>
                <thead>
                    <tr>
                        {% for key in trades[0].keys() %}
                        <th>{{ key }}</th>
                        {% endfor %}
                    </tr>
                </thead>
                <tbody>
                    {% for trade in trades %}
                    <tr>
                        {% for value in trade.values() %}
                        <td>{{ value }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </body>
        </html>
        """
        return render_template_string(html_template, trades=trades)
    except Exception as e:
        logging.error(f"Error retrieving trades: {e}")
        return f"<h1>Error: {e}</h1>", 500

# Initialize database
initialize_database()

# Run Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))