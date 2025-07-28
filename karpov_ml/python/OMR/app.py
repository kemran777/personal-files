from fastapi import FastAPI, HTTPException, Depends
from datetime import date, timedelta
from pydantic import BaseModel
from schema import UserGet, PostGet, FeedGet
from database import Base, SessionLocal
from table_user import User
from table_post import Post
from table_feed import Feed
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List


app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db

@app.get("/user/{id}/feed", response_model=List[FeedGet])
def feed_user_info(id: int, limit: int = 10, db: Session = Depends(get_db)):
    result = (db.query(Feed)
              .filter(Feed.user_id == id)
              .order_by(Feed.time.desc())
              .limit(limit)
              .all())
    return result

@app.get("/post/{id}/feed", response_model=List[FeedGet])
def feed_post_info(id: int, limit: int = 10, db: Session = Depends(get_db)):
    result = (db.query(Feed)
              .filter(Feed.post_id == id)
              .order_by(Feed.time.desc())
              .limit(limit)
              .all())
    return result

@app.get("/post/recommendations/", response_model=List[PostGet])
def post_recommendations(id: int, limit: int = 10, db: Session = Depends(get_db)):
    result = (db.query(Post.id, Post.text, Post.topic, func.count(Feed.post_id).label("like_count"))
              .join(Feed, Feed.post_id == Post.id)
              .filter(Feed.action == 'like')
              .group_by(Post.id, Post.text, Post.topic)
              .order_by(func.count(Feed.post_id).desc())
              .limit(limit)
              .all())
    return [PostGet(id=row.id, text=row.text, topic=row.topic) for row in result]
