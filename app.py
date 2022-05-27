from typing import List, Optional

import uvicorn
from fastapi import FastAPI
#
# from db.config import engine, Base, async_session
# from db.dals.book_dal import BookDAL
# from db.models.book import Book

app = FastAPI()


@app.on_event("startup")
async def startup():
    # create db tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@app.post("/books")
async def create_book(name: str, author: str, release_year: int):
    async with async_session() as session:
        async with session.begin():
            book_dal = BookDAL(session)
            return await book_dal.create_book(name, author, release_year)


@app.get("/books")
async def get_all_books() -> List[Book]:
    async with async_session() as session:
        async with session.begin():
            book_dal = BookDAL(session)
            return await book_dal.get_all_books()

@app.put("/books/{book_id}")
async def update_book(book_id: int, name: Optional[str] = None, author: Optional[str] = None, release_year: Optional[int] = None):
    async with async_session() as session:
        async with session.begin():
            book_dal = BookDAL(session)
            return await book_dal.update_book(book_id, name, author, release_year)


if __name__ == '__main__':
    uvicorn.run("app:app", port=1111, host='127.0.0.1')