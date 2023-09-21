import uvicorn
from fastapi import FastAPI

import routers

app = FastAPI()
app.include_router(routers.router)

if __name__ == '__main__':
    uvicorn.run("app:app", port=1111, host='127.0.0.1', timeout_keep_alive=180)
