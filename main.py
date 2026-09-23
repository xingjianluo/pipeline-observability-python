from fastapi import FastAPI

from pipelineobs.api import router as obs_router

app = FastAPI(title="pipeline-observability")
app.include_router(obs_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
