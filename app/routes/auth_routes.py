from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# Dictionary of valid usernames and passwords
VALID_USERS = {
    "ajit3": "ajit",
    "damodar1": "damodar",
    "admin": "admin",
    "testuser": "testpass",
    "Rajveer": "Rajveer",
    "user1": "user1",
    "user2": "user2",
    "user3": "user3",
    "user4": "user4",
    "user5": "user5"
}

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
async def login(credentials: LoginRequest):
    if credentials.username in VALID_USERS and VALID_USERS[credentials.username] == credentials.password:
        return {"success": True}
    raise HTTPException(status_code=400, detail="Invalid credentials")
