import datetime
from datetime import timedelta
from fastapi import FastAPI


app = FastAPI()
@app.get("/sum_date")
def sum_dates(current_date: datetime.date, offset: int):
    return current_date + timedelta(days=offset)