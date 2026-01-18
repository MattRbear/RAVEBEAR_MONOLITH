"""Tests for read-only Query API endpoints."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ravebear_monolith.api.app import create_app
from ravebear_monolith.collectors.base import CollectorEvent
from ravebear_monolith.foundation.config import AppConfig
from ravebear_monolith.storage.bar_sink import Bar1s, BarSink
from ravebear_monolith.storage.event_sink import EventSink


def make_event(source: str, event_type: str, payload: dict, ts_suffix: int = 0) -> CollectorEvent:
    """Create a test CollectorEvent."""
    return CollectorEvent(
        source=source,
        event_type=event_type,
        ts_utc=f"2024-01-18T12:00:{ts_suffix:02d}+00:00",
        payload=payload,
    )


def make_bar(symbol: str, ts_ms: int, price: float = 100.0) -> Bar1s:
    """Create a test bar."""
    return Bar1s(
        symbol=symbol,
        ts_ms=ts_ms,
        open=price,
        high=price + 10,
        low=price - 10,
        close=price + 5,
        volume=1.0,
        trade_count=10,
    )


async def seed_events(db_path: Path, events: list[CollectorEvent]) -> None:
    """Seed database with events."""
    async with EventSink(db_path) as sink:
        for event in events:
            await sink.write(event)


async def seed_bars(db_path: Path, bars: list[Bar1s]) -> None:
    """Seed database with bars."""
    async with BarSink(db_path) as sink:
        for bar in bars:
            await sink.upsert_bar(bar)


@pytest.fixture
def seeded_db(tmp_path: Path) -> Path:
    """Create seeded database for testing."""
    import asyncio

    db_path = tmp_path / "test.db"

    async def seed():
        # Seed events
        events = [make_event("okx", "trade", {"price": 42000 + i}, ts_suffix=i) for i in range(5)]
        events.extend(
            [make_event("binance", "trade", {"price": 42100 + i}, ts_suffix=i) for i in range(3)]
        )
        await seed_events(db_path, events)

        # Seed bars
        bars = [make_bar("BTC-USDT", 1000 * (i + 1), 42000 + i * 10) for i in range(5)]
        bars.extend([make_bar("ETH-USDT", 1000 * (i + 1), 2000 + i * 5) for i in range(3)])
        await seed_bars(db_path, bars)

    asyncio.get_event_loop().run_until_complete(seed())
    return db_path


@pytest.fixture
def test_config(seeded_db: Path, tmp_path: Path) -> AppConfig:
    """Create test config pointing to seeded database."""
    return AppConfig(
        data_dir=tmp_path,
        storage={"db_path": seeded_db},
        kill_switch_path=tmp_path / "kill.txt",
    )


@pytest.fixture
def client(test_config: AppConfig) -> TestClient:
    """Create test client for API."""
    app = create_app(test_config)
    with TestClient(app) as client:
        yield client


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_ok(self, client: TestClient) -> None:
        """Health endpoint returns ok=true and required fields."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert "ts_utc" in data
        assert "checks" in data
        assert "disk_free" in data["checks"]
        assert "python_version" in data["checks"]
        assert "write_access" in data["checks"]


class TestEventsEndpoint:
    """Tests for /events endpoint."""

    def test_events_returns_seeded_events(self, client: TestClient) -> None:
        """Events endpoint returns all seeded events."""
        response = client.get("/events")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 8  # 5 okx + 3 binance
        assert len(data["items"]) == 8

    def test_events_filter_by_source(self, client: TestClient) -> None:
        """Events can be filtered by source."""
        response = client.get("/events?source=okx")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 5
        assert all(item["source"] == "okx" for item in data["items"])

    def test_events_filter_by_event_type(self, client: TestClient) -> None:
        """Events can be filtered by event_type."""
        response = client.get("/events?event_type=trade")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 8
        assert all(item["event_type"] == "trade" for item in data["items"])

    def test_events_limit(self, client: TestClient) -> None:
        """Events limit is enforced."""
        response = client.get("/events?limit=2")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["items"]) == 2

    def test_events_order_desc(self, client: TestClient) -> None:
        """Events can be ordered descending."""
        response = client.get("/events?order=desc")

        assert response.status_code == 200
        data = response.json()
        items = data["items"]
        # Verify descending order by ts_ms
        for i in range(len(items) - 1):
            assert items[i]["ts_ms"] >= items[i + 1]["ts_ms"]


class TestBarsEndpoint:
    """Tests for /bars/1s endpoint."""

    def test_bars_returns_seeded_bars(self, client: TestClient) -> None:
        """Bars endpoint returns all seeded bars."""
        response = client.get("/bars/1s")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 8  # 5 BTC + 3 ETH
        assert len(data["items"]) == 8

    def test_bars_filter_by_symbol(self, client: TestClient) -> None:
        """Bars can be filtered by symbol."""
        response = client.get("/bars/1s?symbol=BTC-USDT")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 5
        assert all(item["symbol"] == "BTC-USDT" for item in data["items"])

    def test_bars_limit(self, client: TestClient) -> None:
        """Bars limit is enforced."""
        response = client.get("/bars/1s?limit=3")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3


class TestCorrelationId:
    """Tests for correlation ID middleware."""

    def test_correlation_id_echoed(self, client: TestClient) -> None:
        """Provided correlation ID is echoed in response."""
        test_id = "test-correlation-123"
        response = client.get("/health", headers={"x-correlation-id": test_id})

        assert response.status_code == 200
        assert response.headers["x-correlation-id"] == test_id

    def test_correlation_id_generated(self, client: TestClient) -> None:
        """Correlation ID is generated if not provided."""
        response = client.get("/health")

        assert response.status_code == 200
        assert "x-correlation-id" in response.headers
        # Should be a valid UUID format
        correlation_id = response.headers["x-correlation-id"]
        assert len(correlation_id) == 36  # UUID format


class TestValidation:
    """Tests for query parameter validation."""

    def test_events_limit_too_high(self, client: TestClient) -> None:
        """Limit > 50000 returns 422."""
        response = client.get("/events?limit=50001")

        assert response.status_code == 422

    def test_bars_limit_too_high(self, client: TestClient) -> None:
        """Limit > 50000 returns 422."""
        response = client.get("/bars/1s?limit=50001")

        assert response.status_code == 422

    def test_invalid_order(self, client: TestClient) -> None:
        """Invalid order returns 422."""
        response = client.get("/events?order=invalid")

        assert response.status_code == 422
