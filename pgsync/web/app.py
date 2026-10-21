"""PGSync Studio aiohttp application."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import typing as t

from aiohttp import web

from .config_store import ConfigStore
from .db import DatabaseManager
from .external_store import create_store
from .metrics import render_prometheus
from .sync_manager import StudioError, SyncManager

logger = logging.getLogger(__name__)


def static_dir() -> str:
    """Return the static asset directory."""
    return os.path.join(os.path.dirname(__file__), "static")


async def utf8_charset(
    request: web.Request,
    response: web.StreamResponse,
) -> None:
    """Ensure browser-facing text assets are emitted as UTF-8."""
    content_type: str = getattr(response, "content_type", "") or ""
    if getattr(response, "charset", None):
        return
    if content_type.startswith("text/") or content_type in {
        "application/javascript",
        "application/json",
    }:
        response.charset = "utf-8"


@web.middleware
async def error_middleware(
    request: web.Request,
    handler: t.Callable[[web.Request], t.Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except StudioError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    except web.HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unhandled studio error")
        return web.json_response(
            {"ok": False, "error": str(exc)},
            status=500,
        )


async def json_body(request: web.Request) -> dict:
    """Read the request body as JSON."""
    if request.can_read_body:
        payload = await request.text()
        if not payload:
            return {}
        return json.loads(payload)
    return {}


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(os.path.join(static_dir(), "index.html"))


async def favicon(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(os.path.join(static_dir(), "favicon.ico"))


async def bootstrap(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    return web.json_response({"ok": True, "data": manager.bootstrap_payload()})


async def metrics(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    return web.Response(
        text=render_prometheus(manager),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )


async def save_services(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.save_services(payload)
    return web.json_response({"ok": True, "data": data})


async def test_connection(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.test_connection(payload)
    return web.json_response({"ok": True, "data": data})


async def save_connection(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.save_connection(payload)
    return web.json_response({"ok": True, "data": data})


async def delete_connection(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    connection_id: str = request.match_info["connection_id"]
    if not manager.delete_connection(connection_id):
        raise StudioError(f"连接 {connection_id} 不存在。")
    return web.json_response({"ok": True})


async def list_schemas(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    connection_id: str = request.match_info["connection_id"]
    data: list[str] = manager.list_schemas(connection_id)
    return web.json_response({"ok": True, "data": data})


async def list_tables(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    connection_id: str = request.match_info["connection_id"]
    schema: str = request.query.get("schema", "public")
    data: list[str] = manager.list_tables(connection_id, schema)
    return web.json_response({"ok": True, "data": data})


async def get_columns(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    connection_id: str = request.match_info["connection_id"]
    schema: str = request.query.get("schema", "public")
    table: str = request.query.get("table", "")
    if not table:
        raise StudioError("查询参数 table 不能为空。")
    data: dict = manager.get_columns(connection_id, schema, table)
    return web.json_response({"ok": True, "data": data})


async def validate_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.validate_job(payload)
    return web.json_response({"ok": True, "data": data})


async def preview_job_sql(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.preview_job_sql(payload)
    return web.json_response({"ok": True, "data": data})


async def save_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    start: bool = bool(payload.pop("start", True))
    data: dict = manager.save_job(payload, start=start)
    return web.json_response({"ok": True, "data": data})


async def copy_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    source_job_id: str = request.match_info["job_id"]
    payload: dict = await json_body(request)
    data: dict = manager.copy_job(source_job_id, payload)
    return web.json_response({"ok": True, "data": data})


async def delete_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    job_id: str = request.match_info["job_id"]
    payload: dict = await json_body(request)
    data: dict = manager.delete_job(
        job_id,
        delete_index=bool(payload.get("delete_index")),
    )
    return web.json_response({"ok": True, "data": data})


async def start_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    job_id: str = request.match_info["job_id"]
    data: dict = manager.start_job(job_id)
    return web.json_response({"ok": True, "data": data})


async def stop_job(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    job_id: str = request.match_info["job_id"]
    data: dict = manager.stop_job(job_id)
    return web.json_response({"ok": True, "data": data})


async def job_runtime(request: web.Request) -> web.StreamResponse:
    store: ConfigStore = request.app["store"]
    job_id: str = request.match_info["job_id"]
    return web.json_response(
        {"ok": True, "data": store.get_job_runtime(job_id)}
    )


async def job_debug(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    job_id: str = request.match_info["job_id"]
    return web.json_response(
        {"ok": True, "data": manager.get_job_debug(job_id)}
    )


async def search_job_documents(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    job_id: str = request.match_info["job_id"]
    payload: dict = await json_body(request)
    data: dict = manager.search_job_documents(job_id, payload)
    return web.json_response({"ok": True, "data": data})


async def search_index_documents(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.search_index_documents(payload)
    return web.json_response({"ok": True, "data": data})


async def add_index_alias(request: web.Request) -> web.StreamResponse:
    manager: SyncManager = request.app["manager"]
    payload: dict = await json_body(request)
    data: dict = manager.add_index_alias(payload)
    return web.json_response({"ok": True, "data": data})


async def on_startup(app: web.Application) -> None:
    manager: SyncManager = app["manager"]
    manager.start_background_recovery()
    manager.start_connection_auto_discovery_scheduler()
    manager.start_durable_outbox_cleanup_scheduler()
    manager.start_durable_outbox_partition_maintenance_scheduler()


async def on_shutdown(app: web.Application) -> None:
    manager: SyncManager = app["manager"]
    await asyncio.to_thread(manager.prepare_for_shutdown)


def create_app() -> web.Application:
    """Create the aiohttp application."""
    store = create_store()
    db = DatabaseManager()
    manager = SyncManager(
        store=store,
        db=db,
        repo_root=os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        ),
    )

    app = web.Application(middlewares=[error_middleware])
    app["store"] = store
    app["db"] = db
    app["manager"] = manager
    app.on_response_prepare.append(utf8_charset)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    app.add_routes(
        [
            web.get("/", index),
            web.get("/favicon.ico", favicon),
            web.get("/metrics", metrics),
            web.get("/api/bootstrap", bootstrap),
            web.post("/api/services", save_services),
            web.post("/api/connections/test", test_connection),
            web.post("/api/connections", save_connection),
            web.delete("/api/connections/{connection_id}", delete_connection),
            web.get("/api/connections/{connection_id}/schemas", list_schemas),
            web.get("/api/connections/{connection_id}/tables", list_tables),
            web.get("/api/connections/{connection_id}/columns", get_columns),
            web.post("/api/jobs/validate", validate_job),
            web.post("/api/jobs/sql-preview", preview_job_sql),
            web.post("/api/jobs", save_job),
            web.post("/api/jobs/{job_id}/copy", copy_job),
            web.delete("/api/jobs/{job_id}", delete_job),
            web.get("/api/jobs/{job_id}/runtime", job_runtime),
            web.get("/api/jobs/{job_id}/debug", job_debug),
            web.post("/api/jobs/{job_id}/search", search_job_documents),
            web.post("/api/search/index", search_index_documents),
            web.post("/api/search/alias", add_index_alias),
            web.post("/api/jobs/{job_id}/start", start_job),
            web.post("/api/jobs/{job_id}/stop", stop_job),
        ]
    )
    app.router.add_static("/static/", static_dir(), show_index=True)
    return app


def main() -> None:
    """Run the studio web server."""
    host: str = os.environ.get("PGSYNC_STUDIO_HOST", "127.0.0.1")
    port: int = int(os.environ.get("PGSYNC_STUDIO_PORT", "8180"))
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    web.run_app(create_app(), host=host, port=port)


if __name__ == "__main__":
    main()
