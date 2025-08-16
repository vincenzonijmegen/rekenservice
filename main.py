from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"fingerprint": "VINCENZO-HEALTHZ-42"}

@app.get("/healthz")
def healthz():
    return {"ok": True}
