from databases import Database
from sqlalchemy import create_engine, MetaData
from config.config import Config
import os

DATABASE_URL = Config.DATABASE_URL
engine = create_engine(DATABASE_URL)
metadata = MetaData()
database = Database(DATABASE_URL)

# Ensure the logs directory exists
LOGS_DIR = "./logs"
os.makedirs(LOGS_DIR, exist_ok=True)

async def connect_db():
    try:
        await database.connect()
        print("Connected to database.")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

async def disconnect_db():
    try:
        await database.disconnect()
        print("Disconnected from database.")
    except Exception as e:
        print(f"Error disconnecting from database: {e}")
        raise

def create_tables():
    """Create all tables defined in the metadata."""
    metadata.create_all(engine)
    print("Tables created successfully.")