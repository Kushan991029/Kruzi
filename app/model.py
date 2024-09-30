from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class User(BaseModel):
    username: str
    password: str
    weatherSt: List[str]
    digitalTwin: List[str]
    email: str
    name: str
    
class updateUser(BaseModel):
    username: Optional[str] = None
    password: str
    weatherSt: Optional[List[str]] = None
    digitalTwin: Optional[List[str]] = None
    email: Optional[str] = None
    name: Optional[str] = None
    
class loginUser(BaseModel):
    username: str
    password: str
    
class userLog(BaseModel):
    username: str
    timestamp: datetime = None