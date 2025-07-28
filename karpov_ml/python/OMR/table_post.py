from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from database import Base

SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    post = Column(String)
    topic = Column(String)

if __name__ == "__main__":
    session = SessionLocal()
    
    posts = (session.query(Post.id)
                  .filter(Post.topic == "business")
                  .order_by(Post.id.desc())
                  .limit(10)
                  .all())


    result = [post.id for post in posts]
    print(result)
