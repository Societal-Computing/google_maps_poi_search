from fastapi import Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from db.models import users
from db.database import database
from fastapi import Request

# Define the API key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

SESSIONS = {}

async def get_current_user(api_key: str = Security(api_key_header)):
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is missing",
        )
    
    query = users.select().where(users.c.api_key == api_key)
    user = await database.fetch_one(query)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    
    return user

async def get_current_user_from_session(request: Request):
    session_id = request.cookies.get("session_id")
    if not session_id or session_id not in SESSIONS:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return SESSIONS[session_id]