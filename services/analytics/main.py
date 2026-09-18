import os
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg

app = FastAPI(
    title="GoMarket Analytics API",
    description="Python/FastAPI Polyglot Analytical Sidecar for GoMarket Event Engine",
    version="1.0.0",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://postgres:postgres@postgres:5432/gomarket?sslmode=disable",
)


class AnalyticsSummary(BaseModel):
    total_signals: int
    avg_spread_pct: float
    max_spread_pct: float
    top_symbol: str
    status: str


class SymbolMetric(BaseModel):
    symbol: str
    signal_count: int
    avg_spread_pct: float


pool: asyncpg.Pool = None


@app.on_event("startup")
async def startup_db():
    global pool
    try:
        dsn = DATABASE_URL.replace("postgres://", "postgresql://")
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    except Exception as e:
        print(f"Warning: Could not connect to Postgres at startup: {e}")


@app.on_event("shutdown")
async def shutdown_db():
    global pool
    if pool:
        await pool.close()


@app.get("/healthz")
async def health_check() -> Dict[str, str]:
    return {"status": "ok", "service": "analytics-sidecar"}


@app.get("/api/v1/analytics/summary", response_model=AnalyticsSummary)
async def get_summary():
    if not pool:
        return AnalyticsSummary(
            total_signals=0,
            avg_spread_pct=0.0,
            max_spread_pct=0.0,
            top_symbol="N/A",
            status="database_unavailable",
        )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT 
                COUNT(*) as total_signals,
                COALESCE(AVG(spread_pct), 0.0) as avg_spread_pct,
                COALESCE(MAX(spread_pct), 0.0) as max_spread_pct
            FROM signals;
        """
        )
        top_row = await conn.fetchrow(
            """
            SELECT symbol
            FROM signals
            GROUP BY symbol
            ORDER BY COUNT(*) DESC
            LIMIT 1;
        """
        )

        top_symbol = top_row["symbol"] if top_row else "N/A"

        return AnalyticsSummary(
            total_signals=row["total_signals"],
            avg_spread_pct=round(float(row["avg_spread_pct"]), 4),
            max_spread_pct=round(float(row["max_spread_pct"]), 4),
            top_symbol=top_symbol,
            status="healthy",
        )


@app.get("/api/v1/analytics/symbols", response_model=List[SymbolMetric])
async def get_symbol_metrics():
    if not pool:
        return []

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT 
                symbol,
                COUNT(*) as signal_count,
                COALESCE(AVG(spread_pct), 0.0) as avg_spread_pct
            FROM signals
            GROUP BY symbol
            ORDER BY signal_count DESC;
        """
        )
        return [
            SymbolMetric(
                symbol=r["symbol"],
                signal_count=r["signal_count"],
                avg_spread_pct=round(float(r["avg_spread_pct"]), 4),
            )
            for r in rows
        ]
