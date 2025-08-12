from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware  # Garante que esta linha existe
import uuid
from typing import Dict

from .processing import processar_dados

app = FastAPI(
    title="PGFN Leads API",
    description="API para processar dados de devedores da PGFN e cruzá-los com dados de parcelamento.",
    version="1.0.0"
)

# --- CONFIGURAÇÃO DE CORS DO PROJETO FUNCIONAL ---
# Define que qualquer origem pode acessar sua API
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# --- FIM DA CONFIGURAÇÃO DE CORS ---

# O resto do seu código permanece o mesmo...
jobs: Dict[str, Dict] = {}

def run_processing_task(job_id: str, valor_minimo: float, file_bytes: bytes):
    try:
        resultado = processar_dados(valor_minimo, file_bytes)
        jobs[job_id]["status"] = "concluido"
        jobs[job_id]["result"] = resultado
    except Exception as e:
        jobs[job_id]["status"] = "erro"
        jobs[job_id]["result"] = str(e)

@app.post("/processar", status_code=202)
async def iniciar_processamento(background_tasks: BackgroundTasks, valor_minimo: float = Form(...), file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    file_bytes = await file.read()
    jobs[job_id] = {"status": "processando"}
    background_tasks.add_task(run_processing_task, job_id, valor_minimo, file_bytes)
    return {"job_id": job_id, "message": "Processamento iniciado."}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return {"job_id": job_id, "status": job["status"]}

@app.get("/resultado/{job_id}")
async def get_resultado(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    if job["status"] != "concluido":
        raise HTTPException(status_code=400, detail=f"Job ainda não concluído. Status atual: {job['status']}")
    return {"job_id": job_id, "resultado": job["result"]}