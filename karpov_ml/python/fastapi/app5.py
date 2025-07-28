from fastapi import FastAPI
from pydantic import BaseModel
from datetime import date

app = FastAPI()

class User(BaseModel):
    name: str
    surname: str
    age: int
    registration_date: date

@app.post("/user/validate")
def validate_user(user: User):
    return {"message": "User validated", "user": user}