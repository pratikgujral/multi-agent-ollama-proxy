import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

import ollama_router
from ollama_router import app


client = TestClient(app)


def create_database(path: Path, nodes=(), models=()):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE nodes (ip TEXT PRIMARY KEY, last_seen DATETIME)"
        )
        connection.execute(
            "CREATE TABLE models (node_ip TEXT, model_name TEXT, "
            "PRIMARY KEY (node_ip, model_name))"
        )
        connection.executemany(
            "INSERT INTO nodes (ip, last_seen) VALUES (?, datetime('now'))",
            [(ip,) for ip in nodes],
        )
        connection.executemany(
            "INSERT INTO models (node_ip, model_name) VALUES (?, ?)",
            models,
        )
        connection.commit()


class TestDashboardEndpoint:
    def test_dashboard_returns_html(self):
        response = client.get("/dashboard")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "Ollama Cluster Live Monitor" in response.text

    def test_dashboard_contains_expected_elements(self):
        response = client.get("/dashboard")

        assert "<!DOCTYPE html>" in response.text
        assert 'id="cluster-grid"' in response.text
        assert 'id="node-count"' in response.text
        assert "EventSource" in response.text
        assert "Connection lost. Reconnecting..." in response.text

    def test_dashboard_has_valid_structure(self):
        response = client.get("/dashboard")

        assert "<html" in response.text
        assert "<head>" in response.text
        assert "<body>" in response.text
        assert "<title>Ollama Cluster Live Monitor</title>" in response.text


class TestSSEEndpoint:
    def test_sse_endpoint_streams_current_cluster_state(self):
        status = {"active_nodes": [{"ip": "10.0.0.1", "models": ["llama3"]}]}

        class Request:
            checks = 0

            async def is_disconnected(self):
                self.checks += 1
                return self.checks > 1

        async def exercise_stream():
            with patch(
                "ollama_router.fetch_current_cluster_state",
                new=AsyncMock(return_value=status),
            ):
                response = await ollama_router.stream_cluster_status(Request())
                event = await response.body_iterator.__anext__()
                return response, event

        response, event = asyncio.run(exercise_stream())

        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["connection"] == "keep-alive"
        assert json.loads(event.removeprefix("data: ").strip()) == status

    def test_sse_endpoint_stops_when_client_disconnects(self):
        class Request:
            async def is_disconnected(self):
                return True

        async def exercise_stream():
            response = await ollama_router.stream_cluster_status(Request())
            with pytest.raises(StopAsyncIteration):
                await response.body_iterator.__anext__()

        asyncio.run(exercise_stream())


class TestClusterStateFetching:
    def test_fetch_current_cluster_state_returns_empty_when_no_nodes(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(database)

        with patch.object(ollama_router, "DB_FILE", str(database)):
            result = asyncio.run(ollama_router.fetch_current_cluster_state())

        assert result == {"active_nodes": []}

    def test_fetch_current_cluster_state_reports_online_models(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(database, nodes=["10.0.0.1"])
        response = MagicMock(
            status_code=200,
            json=lambda: {"models": [{"name": "llama3"}, {"name": "mistral"}]},
        )
        client_context = MagicMock()
        client_context.__aenter__ = AsyncMock(return_value=client_context)
        client_context.__aexit__ = AsyncMock(return_value=None)
        client_context.get = AsyncMock(return_value=response)

        with (
            patch.object(ollama_router, "DB_FILE", str(database)),
            patch("ollama_router.httpx.AsyncClient", return_value=client_context),
        ):
            result = asyncio.run(ollama_router.fetch_current_cluster_state())

        assert result == {
            "active_nodes": [
                {"ip": "10.0.0.1", "models": ["llama3", "mistral"]}
            ]
        }
        client_context.get.assert_awaited_once_with(
            "http://10.0.0.1:11434/api/tags"
        )

    @pytest.mark.parametrize(
        "failure",
        [asyncio.TimeoutError(), httpx.RequestError("offline")],
    )
    def test_fetch_current_cluster_state_excludes_unreachable_nodes(
        self, tmp_path, failure
    ):
        database = tmp_path / "cluster.db"
        create_database(database, nodes=["10.0.0.1"])
        client_context = MagicMock()
        client_context.__aenter__ = AsyncMock(return_value=client_context)
        client_context.__aexit__ = AsyncMock(return_value=None)
        client_context.get = AsyncMock(side_effect=failure)

        with (
            patch.object(ollama_router, "DB_FILE", str(database)),
            patch("ollama_router.httpx.AsyncClient", return_value=client_context),
        ):
            result = asyncio.run(ollama_router.fetch_current_cluster_state())

        assert result == {"active_nodes": []}

    def test_fetch_current_cluster_state_excludes_http_errors(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(database, nodes=["10.0.0.1"])
        response = MagicMock(status_code=500)
        client_context = MagicMock()
        client_context.__aenter__ = AsyncMock(return_value=client_context)
        client_context.__aexit__ = AsyncMock(return_value=None)
        client_context.get = AsyncMock(return_value=response)

        with (
            patch.object(ollama_router, "DB_FILE", str(database)),
            patch("ollama_router.httpx.AsyncClient", return_value=client_context),
        ):
            result = asyncio.run(ollama_router.fetch_current_cluster_state())

        assert result == {"active_nodes": []}


class TestConfiguration:
    def test_get_target_subnets_with_valid_config(self, tmp_path):
        config = tmp_path / "config.json"
        config.write_text(
            json.dumps({"subnets": ["10.237.0.0/16", "10.208.0.0/16"]}),
            encoding="utf-8",
        )

        with patch.object(ollama_router, "CONFIG_FILE", str(config)):
            result = ollama_router.get_target_subnets()

        assert result == ["10.237.0.0/16", "10.208.0.0/16"]

    def test_get_target_subnets_returns_empty_for_missing_config(self, tmp_path):
        with patch.object(
            ollama_router, "CONFIG_FILE", str(tmp_path / "missing.json")
        ):
            result = ollama_router.get_target_subnets()

        assert result == []

    def test_get_target_subnets_returns_empty_for_invalid_json(self, tmp_path):
        config = tmp_path / "config.json"
        config.write_text("{invalid", encoding="utf-8")

        with patch.object(ollama_router, "CONFIG_FILE", str(config)):
            result = ollama_router.get_target_subnets()

        assert result == []


class TestClusterEndpoints:
    def test_cluster_status_aggregates_models_by_node(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(
            database,
            models=[
                ("10.0.0.1", "llama3"),
                ("10.0.0.1", "mistral"),
                ("10.0.0.2", "phi3"),
            ],
        )

        with patch.object(ollama_router, "DB_FILE", str(database)):
            response = client.get("/cluster/status")

        assert response.status_code == 200
        assert response.json() == {
            "active_nodes": [
                {"ip": "10.0.0.1", "models": ["llama3", "mistral"]},
                {"ip": "10.0.0.2", "models": ["phi3"]},
            ]
        }

    def test_api_tags_returns_deduplicated_ollama_model_metadata(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(
            database,
            models=[
                ("10.0.0.1", "llama3:8b"),
                ("10.0.0.2", "llama3:8b"),
                ("10.0.0.2", "mistral"),
            ],
        )

        with patch.object(ollama_router, "DB_FILE", str(database)):
            response = client.get("/api/tags")

        models = response.json()["models"]
        assert response.status_code == 200
        assert [model["name"] for model in models] == ["llama3:8b", "mistral"]
        assert models[0]["details"]["family"] == "llama3"
        assert models[1]["details"]["family"] == "mistral"

    def test_manual_scan_trigger_sets_event(self):
        ollama_router.trigger_scan_event.clear()

        response = client.post("/cluster/scan")

        assert response.status_code == 200
        assert response.json() == {"message": "Manual scan triggered successfully."}
        assert ollama_router.trigger_scan_event.is_set()
        ollama_router.trigger_scan_event.clear()

    def test_manual_scan_trigger_rejects_duplicate_trigger(self):
        ollama_router.trigger_scan_event.set()

        response = client.post("/cluster/scan")

        assert response.status_code == 409
        assert response.json() == {"message": "Scan already queued or in progress."}
        ollama_router.trigger_scan_event.clear()


class TestRouting:
    def test_get_node_for_model_returns_matching_node(self, tmp_path):
        database = tmp_path / "cluster.db"
        create_database(database, models=[("10.0.0.1", "llama3")])

        with patch.object(ollama_router, "DB_FILE", str(database)):
            assert ollama_router.get_node_for_model("llama3") == "10.0.0.1"

    @pytest.mark.parametrize(
        "model, status_code, detail",
        [
            ("missing", 404, "Model 'missing' not found"),
            ("", 400, "Missing 'model' parameter in payload"),
        ],
    )
    def test_get_node_for_model_rejects_invalid_or_unknown_models(
        self, tmp_path, model, status_code, detail
    ):
        database = tmp_path / "cluster.db"
        create_database(database)

        with patch.object(ollama_router, "DB_FILE", str(database)):
            with pytest.raises(ollama_router.HTTPException) as error:
                ollama_router.get_node_for_model(model)

        assert error.value.status_code == status_code
        assert error.value.detail == detail
