from __future__ import annotations

import ast
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGES_ROOT = REPO_ROOT / "langbridge" / "packages"
PYTHON_BOUNDARY_ROOTS = [
    REPO_ROOT / "langbridge",
    REPO_ROOT / "tests",
    REPO_ROOT / "scripts",
]
FORBIDDEN_PATHS = [
    REPO_ROOT / "langbridge" / "apps" / "api",
    REPO_ROOT / "langbridge" / "apps" / "worker",
    REPO_ROOT / "alembic",
    REPO_ROOT / "monitoring",
    REPO_ROOT / "langbridge" / "requirements-migrate.txt",
]
FORBIDDEN_IMPORT_PREFIXES = (
    "langbridge.apps.api",
    "langbridge.apps.worker",
    "langbridge_cloud_api",
    "langbridge_cloud_worker",
)


def _is_forbidden(module_name: str | None) -> bool:
    if module_name is None:
        return False
    return module_name == "langbridge.apps" or module_name.startswith("langbridge.apps.")


def _is_forbidden_runtime_import(module_name: str | None) -> bool:
    if module_name is None:
        return False
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in FORBIDDEN_IMPORT_PREFIXES
    )


def _iter_python_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def main() -> int:
    violations: list[tuple[Path, int, str]] = []
    missing_path_violations: list[Path] = []

    for path in FORBIDDEN_PATHS:
        if path.exists():
            missing_path_violations.append(path)

    for path in _iter_python_files(PACKAGES_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden(alias.name):
                        violations.append((path, node.lineno, alias.name))
            elif isinstance(node, ast.ImportFrom):
                if _is_forbidden(node.module):
                    violations.append((path, node.lineno, node.module or ""))

    for root in PYTHON_BOUNDARY_ROOTS:
        for path in _iter_python_files(root):
            tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if _is_forbidden_runtime_import(alias.name):
                            violations.append((path, node.lineno, alias.name))
                elif isinstance(node, ast.ImportFrom):
                    if _is_forbidden_runtime_import(node.module):
                        violations.append((path, node.lineno, node.module or ""))

    if not violations and not missing_path_violations:
        print("runtime boundary check passed")
        return 0

    print("runtime boundary violations detected:", file=sys.stderr)
    for path in missing_path_violations:
        relative_path = path.relative_to(REPO_ROOT)
        print(f"  forbidden path exists: {relative_path}", file=sys.stderr)
    for path, lineno, module_name in violations:
        relative_path = path.relative_to(REPO_ROOT)
        print(f"  {relative_path}:{lineno}: {module_name}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
