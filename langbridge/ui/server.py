from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


def register_runtime_ui(app: FastAPI) -> None:
    static_dir = Path(__file__).resolve().parent / "static"
    assets_dir = static_dir / "assets"
    index_path = static_dir / "index.html"

    app.mount(
        "/ui/assets",
        StaticFiles(directory=str(assets_dir)),
        name="runtime-ui-assets",
    )

    @app.get("/", include_in_schema=False)
    async def runtime_ui_index() -> FileResponse:
        return FileResponse(index_path)

    @app.get("/ui", include_in_schema=False)
    async def runtime_ui_shell() -> FileResponse:
        return FileResponse(index_path)

    @app.get("/{path:path}", include_in_schema=False)
    async def runtime_ui_spa(path: str) -> FileResponse:
        normalized = str(path or "").strip("/")
        if normalized.startswith("api") or normalized.startswith("mcp"):
            raise HTTPException(status_code=404)
        return FileResponse(index_path)
