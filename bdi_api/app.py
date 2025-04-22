import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uptrace
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette import status
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import bdi_api
from bdi_api.examples import v0_router

# from bdi_api.s1.exercise import s1
from bdi_api.s1.exercise import s1
from bdi_api.s4.exercise import s4
from bdi_api.s7.exercise import s7
from bdi_api.settings import Settings
from bdi_api.s8.exercise import s8

logger = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator:
    logger.setLevel(logging.INFO)
    logger.info("Application started. You can check the documentation in http://localhost:8000/docs/")
    yield
    # Shut Down
    logger.warning("Application shutdown")


description = """
# Welcome to the Aircraft API

We'll evolve this application through our class,
from a small app we have running on our laptop
to deployed service.

Besides the technologies we'll see in the course
(AWS, Doceker, PostgreSQL, Airflow) and FastAPI,
feel free to use any python data processing library you are
used to: pandas, polars, duckDB, SQLite, plain python...
Or even best: explore a new one!

Inside the `sX` folders you'll find a `README.md` with
further explanation on the assignment.
"""

app = FastAPI(
    title="Big Data Infrastructure API",
    description="API for the Big Data Infrastructure course",
    version="0.1.0",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

settings = Settings()

if settings.telemetry:
    uptrace.configure_opentelemetry(
        # Copy DSN here or use UPTRACE_DSN env var.
        dsn=Settings().telemetry_dsn,
        service_name=bdi_api.__name__,
        service_version=bdi_api.__version__,
        logging_level=logging.INFO,
    )
    FastAPIInstrumentor.instrument_app(app)
app.include_router(v0_router)
app.include_router(s1)
app.include_router(s4)
app.include_router(s7)
app.include_router(s8)


@app.get("/health", status_code=200)
async def get_health() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content="ok",
    )


@app.get("/version", status_code=200)
async def get_version() -> dict:
    return {"version": bdi_api.__version__}


@app.get("/")
def read_root():
    return {"message": "Welcome to the Big Data Infrastructure API"}


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, proxy_headers=True, access_log=False)


if __name__ == "__main__":
    main()
