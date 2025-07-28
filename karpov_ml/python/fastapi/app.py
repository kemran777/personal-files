from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

# взяли из прошлого задания
def get_db():
    with psycopg2.connect(
            dbname="startml",
            host="postgres.lab.karpov.courses",
            user="robot-startml-ro",
            password="pheiph0hahj1Vaif",
            port=6432,
            cursor_factory=RealDictCursor,
    ) as conn:
        return conn

# объявляем новый класс для валидации ответа
class PostResponse(BaseModel):
    id: int
    text: str
    topic: str

# указываем класс в reponse_model
app = FastAPI()
# далее создаем endpoint аналогично предыдущим заданиям
@app.get("/post/{id}", response_model=PostResponse)
def get_user(id: int, db=Depends(get_db)):
    with db.cursor() as cursor:
        cursor.execute(
            """SELECT id, text, topic FROM "post" WHERE id=%(post_id)s""",
            {"post_id": id},
        )
        result = cursor.fetchone()
    if result is None:
        raise HTTPException(404, "post not found")
    # создаем хитро. Требуется выдержать синтаксис вида PostResponse(id=..., text="текст", topic="топик")
    # но можно поступить лениво: взять словарь {"id": 2, "text": "ttteeexxxtt", "topic": "топик"}
    # и "распаковать" его в вызов что_то(id=2, text="ttteeexxxtt", topic="топик")
    # такую хитрую синтаксическую "распаковку" и делает ** перед словарем
    return PostResponse(**result)
