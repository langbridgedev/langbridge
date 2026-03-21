#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
INTERNAL_DISTRIBUTIONS = {
    "langbridge",
    "langbridge-common",
    "langbridge-connectors",
    "langbridge-contracts",
    "langbridge-federation",
    "langbridge-messaging",
    "langbridge-orchestrator",
    "langbridge-runtime",
    "langbridge-sdk",
    "langbridge-semantic",
}
IGNORE_PATTERNS = shutil.ignore_patterns(
    ".git",
    ".cache",
    ".venv",
    ".venv-*",
    "__pycache__",
    ".pytest_cache",
    "build",
    "dist",
    "*.egg-info",
    "*.pyc",
)
PUBLISHABLE_PACKAGE_DIRS = [
    Path("."),
    Path("packages/sdk"),
]
PUBLISHABLE_PACKAGE_GLOBS = [
    "langbridge-connectors/*/pyproject.toml",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build publishable runtime packages.")
    parser.add_argument("--version", required=True, help="Package version to stamp into all runtime packages.")
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "dist" / "runtime-packages"),
        help="Directory where wheels and sdists are written.",
    )
    return parser.parse_args()


def patch_pyproject(pyproject_path: Path, version: str) -> None:
    text = pyproject_path.read_text(encoding="utf-8")
    text, version_count = re.subn(
        r'(?m)^version = "[^"]+"$',
        f'version = "{version}"',
        text,
        count=1,
    )
    if version_count != 1:
        raise RuntimeError(f"Expected exactly one version declaration in {pyproject_path}")

    for distribution in INTERNAL_DISTRIBUTIONS:
        text = re.sub(
            rf"{re.escape(distribution)}\s*@\s*file:[^\"\n]+",
            f"{distribution}=={version}",
            text,
        )
        text = re.sub(
            rf"{re.escape(distribution)}==[^\"\n,]+",
            f"{distribution}=={version}",
            text,
        )

    pyproject_path.write_text(text, encoding="utf-8")


def iter_package_dirs() -> list[Path]:
    package_dirs: list[Path] = []
    seen: set[Path] = set()

    for package_dir in PUBLISHABLE_PACKAGE_DIRS:
        pyproject_path = REPO_ROOT / package_dir / "pyproject.toml"
        if pyproject_path.exists() and package_dir not in seen:
            package_dirs.append(package_dir)
            seen.add(package_dir)

    for pattern in PUBLISHABLE_PACKAGE_GLOBS:
        for pyproject_path in sorted(REPO_ROOT.glob(pattern)):
            package_dir = pyproject_path.parent.relative_to(REPO_ROOT)
            if package_dir not in seen:
                package_dirs.append(package_dir)
                seen.add(package_dir)

    if not package_dirs:
        raise RuntimeError("No publishable package directories were found.")

    return package_dirs


def build_packages(version: str, output_dir: Path) -> None:
    package_dirs = iter_package_dirs()
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="langbridge-package-build-") as temp_dir:
        temp_root = Path(temp_dir) / "langbridge"
        shutil.copytree(REPO_ROOT, temp_root, ignore=IGNORE_PATTERNS)

        for package_dir in package_dirs:
            patch_pyproject(temp_root / package_dir / "pyproject.toml", version)

        for package_dir in package_dirs:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "build",
                    "--sdist",
                    "--wheel",
                    "--outdir",
                    str(output_dir.resolve()),
                ],
                cwd=temp_root / package_dir,
                check=True,
            )


def main() -> None:
    args = parse_args()
    build_packages(version=args.version, output_dir=Path(args.output_dir))


if __name__ == "__main__":
    main()
