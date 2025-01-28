import os
import logging
import threading
from flask import Flask, render_template_string
from sqlalchemy import create_engine, text

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Environment configuration (Heroku provides DATABASE_URL)
DATABASE_URL = os.environ.get(
    "DATABASE_URL", 
    "postgres://u6nc4l97ds0u0b:p246d69f559a0af43f9a277e314127325b16e50d2d0c618b2e6670b50502f2ef5@c2ihhcf1divl18.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d8uirubbaqeds3"
).replace("postgres://", "postgresql://")

# Function to create the database table
def initialize_database():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        token_name VARCHAR(255),
        volume FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            logging.info("Trades table created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")

# Health check route
@app.route('/')
def health_check():
    return "DEX Monitor Operational"

# Route to display table data
@app.route('/data')
def view_data():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM trades LIMIT 50"))
            data = [dict(row) for row in result]

        # If no data, return message
        if not data:
            return "<h1>No data available in the trades table.</h1>"

        # Render data in an HTML table
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Trades Data</title>
        </head>
        <body>
            <h1>Trades Data</h1>
            <table border="1">
                <tr>{% for key in data[0].keys() %}<th>{{ key }}</th>{% endfor %}</tr>
                {% for row in data %}
                <tr>{% for value in row.values() %}<td>{{ value }}</td>{% endfor %}</tr>
                {% endfor %}
            </table>
        </body>
        </html>
        """
        return render_template_string(html_template, data=data)
    except Exception as e:
        return f"<h1>Error: {e}</h1>", 500

# Initialize database on app startup
if __name__ == "__main__":
    initialize_database()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))