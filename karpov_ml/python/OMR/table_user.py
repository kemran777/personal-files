from sqlalchemy import Column, Integer, String, Boolean, create_engine, desc,
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func


SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

from database import Base

session = SessionLocal()

class User(Base):
    __tablename__ = "user"
    __table_args__ = {"schema": "public"}
    id = Column(Integer, primary_key=True)
    gender = Column(Integer)
    age = Column(Integer)
    country = Column(String)
    city = Column(String)
    exp_group = Column(Integer)
    os = Column(String)
    source = Column(String)

if __name__ == "__main__":
    Base.metadata.create_all(engine)

    print(
        session.query(User.country, User.os, func.count("*"))
        .filter(User.exp_group == 3)
        .group_by(User.country, User.os)
        .having(func.count("*")>100)
        .order_by(desc(func.count("*")))
        .all()
        )
