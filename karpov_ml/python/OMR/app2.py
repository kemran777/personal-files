from fastapi import FastAPI, HTTPException, Depends
from datetime import date, timedelta
from pydantic import BaseModel
from schema import UserGet, PostGet
from database import Base, SessionLocal
from table_user import User
from table_post import Post
from sqlalchemy.orm import Session

app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db

@app.get("/user/{id}", response_model=UserGet)
def get_user_info(id: int, db: Session = Depends(get_db)):
    result = db.query(User).filter(User.id == id).one_or_none()
    if not result:
        raise HTTPException(404, "user not found")
    else:
        return UserGet(**{key: value for key, value in result.__dict__.items() if not key.startswith("_")})


@app.get("/post/{id}", response_model=PostGet)
def get_post_info(id: int, db: Session = Depends(get_db)):
    result = db.query(Post).filter(Post.id == id).one_or_none()
    if not result:
        raise HTTPException(404, "post not found")
    else:
        return PostGet(**{key: value for key, value in result.__dict__.items() if not key.startswith("_")})
