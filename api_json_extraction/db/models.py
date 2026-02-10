from sqlalchemy import Table, Column, Integer, String, Boolean, TIMESTAMP, DateTime, JSON, ForeignKey
from sqlalchemy.sql.expression import text
from passlib.context import CryptContext
from db.database import metadata
import secrets

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# db/models.py
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("username", String, unique=True, index=True),
    Column("hashed_password", String),
    Column("api_key", String, unique=True, nullable=True),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

# Jobs table
jobs = Table(
    "jobs",
    metadata,
    Column("id", String, primary_key=True),
    Column("username", String, ForeignKey("users.username")),
    Column("status", String),
    Column("timestamp", DateTime),
    Column("last_updated", DateTime),
    Column("file_path", String),
    Column("output_file", String, nullable=True),
    Column("total_place_ids", Integer, nullable=True),
    Column("processed_place_ids", Integer, default=0),
    Column("extraction_options", JSON, nullable=True),
    Column("error_message", String, nullable=True),
)

def hash_password(password: str) -> str:
    try:
        hashed_password = pwd_context.hash(password)
        return hashed_password
    except Exception as e:
        print(f"Error hashing password: {e}")
        raise

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)