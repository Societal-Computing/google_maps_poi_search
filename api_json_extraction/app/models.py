from pydantic import BaseModel

class UserCreate(BaseModel):
    username: str
    password: str

class ProxyRelease(BaseModel):
    proxy: str

class ProxyRemove(BaseModel):
    proxy: str