from fastapi import FastAPI

# Создаем экземпляр приложения FastAPI
app = FastAPI()

# Определяем endpoint для корня "/" с методом GET
@app.get("/")
def read_root():
    return "hello, world"