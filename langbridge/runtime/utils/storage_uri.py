
from pathlib import Path
from urllib.parse import unquote, urlparse


def path_to_storage_uri(path: str | Path) -> str:
    return Path(path).resolve().as_uri()


def resolve_local_storage_path(storage_uri: str) -> Path:
    parsed = urlparse(storage_uri)
    if parsed.scheme in {"", "file"}:
        raw_path = parsed.path or storage_uri
        if len(raw_path) >= 3 and raw_path[0] == "/" and raw_path[2] == ":":
            raw_path = raw_path[1:]
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            raw_path = f"//{parsed.netloc}{raw_path}"
        return Path(unquote(raw_path)).resolve()
    raise ValueError(f"Unsupported storage URI scheme '{parsed.scheme}'.")
