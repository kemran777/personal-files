from fastapi import FastAPI, Query

app = FastAPI()

@app.get("/")
def sum_two(a: int, b: int):
    return a+b
