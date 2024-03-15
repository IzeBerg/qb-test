from fastapi import FastAPI

from api import instruments

app = FastAPI()
app.include_router(instruments.router, prefix="/api")
