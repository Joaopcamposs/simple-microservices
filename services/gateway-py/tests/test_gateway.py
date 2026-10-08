import httpx
import pytest
from app.main import create_app, get_repository
from app.models import JobView


class FakeRepository:
    async def create(self, request, origin):  # pragma: no cover - não deve ser chamado
        raise AssertionError("não deveria gravar")

    async def get(self, job_id) -> JobView | None:
        return None


@pytest.fixture
def client() -> httpx.AsyncClient:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: FakeRepository()
    # ASGITransport não roda o lifespan, então não precisa de banco.
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        ({}, "type is required"),
        ({"type": ""}, "type is required"),
        ({"payload": {"a": 1}}, "type is required"),
    ],
)
async def test_create_job_rejects_missing_type(client, body, detail) -> None:
    response = await client.post("/jobs", json=body)
    assert response.status_code == 422
    assert response.json() == {"detail": detail}


async def test_create_job_rejects_invalid_json(client) -> None:
    response = await client.post(
        "/jobs", content="not json", headers={"content-type": "application/json"}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid request body"}


async def test_get_job_rejects_invalid_id(client) -> None:
    response = await client.get("/jobs/not-a-uuid")
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid job id"}


async def test_get_unknown_job_is_404(client) -> None:
    response = await client.get("/jobs/33333333-3333-3333-3333-333333333333")
    assert response.status_code == 404
    assert response.json() == {"detail": "job not found"}
