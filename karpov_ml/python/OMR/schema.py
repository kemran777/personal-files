from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class UserGet(BaseModel):
    id: int
    gender: Optional[int] = None
    age: Optional[int] = None
    country: Optional[str] = None
    city: Optional[str] = None
    exp_group: Optional[int] = None
    os: Optional[str] = None
    source: Optional[str] = None

    class Config:
        orm_mode = True

class PostGet(BaseModel):
    id: int
    post: str  
    text: str
    topic: Optional[str] = None

    class Config:
        orm_mode = True

class FeedGet(BaseModel):
    user_id: int
    post_id: int
    action: str
    time: datetime

    class Config:
        orm_mode = True
