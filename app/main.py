from fastapi import FastAPI
from analytics_routes import router as analytics_router

app = FastAPI(title="API Consultas Analíticas")

app.include_router(analytics_router)

@app.get("/")
def root():
    return {"message": "API de Consultas Analíticas lista"}
