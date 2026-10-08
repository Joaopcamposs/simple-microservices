"""gateway-py: API FastAPI que grava job + outbox e responde 202."""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.logs import JsonFormatter
from app.models import CreateJobRequest, CreateJobResponse, JobView
from app.repository import JobRepository

ORIGIN = "gateway-py"

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Liga o log JSON e abre o pool no boot; fecha o pool no shutdown. Repositório em app.state."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(ORIGIN))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    # Os loggers do uvicorn têm handler próprio e não propagam; sem ele, vão para o root (JSON).
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
    dsn = os.environ.get("DATABASE_URL", "postgres://app:app@localhost:55432/app")
    async with JobRepository.create_pool(dsn) as pool:
        await pool.open()
        app.state.repository = JobRepository(pool)
        yield


def get_repository(request: Request) -> JobRepository:
    """Dependência: entrega o repositório criado no lifespan (substituível em testes)."""
    return request.app.state.repository


def create_app() -> FastAPI:
    """Monta a aplicação e registra as rotas."""
    app = FastAPI(title="Gateway Py", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, error: RequestValidationError) -> JSONResponse:
        """Responde 422 como {"detail": "<texto curto>"}, igual ao gateway-go."""
        locations = [tuple(item["loc"]) for item in error.errors()]
        if ("path", "job_id") in locations:
            detail = "invalid job id"
        elif ("body", "type") in locations:
            detail = "type is required"
        else:
            detail = "invalid request body"
        return JSONResponse(status_code=422, content={"detail": detail})

    @app.post(
        "/jobs",
        status_code=202,
        response_model=CreateJobResponse,
        tags=["jobs"],
        summary="Cria um job",
        description="Grava job e outbox numa transação e responde 202. Processamento assíncrono.",
    )
    async def create_job(
        request: CreateJobRequest, repository: JobRepository = Depends(get_repository)
    ) -> CreateJobResponse:
        """Aceita o job; o relay/router/worker cuidam do resto."""
        job_id = await repository.create(request, ORIGIN)
        logger.info("job accepted", extra={"job_id": job_id})
        return CreateJobResponse(job_id=job_id)

    @app.get(
        "/jobs/{job_id}",
        response_model=JobView,
        tags=["jobs"],
        summary="Consulta um job",
        description="Devolve o status e os resultados gravados pelos workers.",
    )
    async def get_job(job_id: UUID, repository: JobRepository = Depends(get_repository)) -> JobView:
        """Consulta o job; 404 se não existir."""
        view = await repository.get(job_id)
        if view is None:
            raise HTTPException(status_code=404, detail="job not found")
        return view

    return app


app = create_app()
