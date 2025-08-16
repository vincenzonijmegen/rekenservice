from fastapi import FastAPI, Header, HTTPException
import os

app = FastAPI()
API_TOKEN = os.getenv("API_REKEN_TOKEN", "")

@app.get("/")
def root():
    return {"greeting": "Hello, World!", "message": "Welcome to FastAPI!"}
    
@app.get("/healthz")
def healthz():
    return {"ok": True}

def _auth(authorization: str | None):
    if not API_TOKEN:
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if authorization.split()[1] != API_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")

@app.post("/forecast/day")
def forecast(payload: dict, authorization: str | None = Header(None)):
    _auth(authorization)
    return {"ok": True, "date": payload.get("date")}

@app.post("/optimize/day")
def optimize(payload: dict, authorization: str | None = Header(None)):
    _auth(authorization)
    return {"ok": True, "date": payload.get("date"), "msg": "stub"}
