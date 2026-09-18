import pytest
from httpx import AsyncClient
from main import app


@pytest.mark.asyncio
async def test_healthz():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "analytics-sidecar"}


@pytest.mark.asyncio
async def test_summary_graceful_fallback():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/api/v1/analytics/summary")
    assert response.status_code == 200
    data = response.json()
    assert "total_signals" in data
    assert "status" in data


@pytest.mark.asyncio
async def test_symbols_graceful_fallback():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/api/v1/analytics/symbols")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
