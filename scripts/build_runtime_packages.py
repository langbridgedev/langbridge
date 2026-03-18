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
PACKAGE_DIRS = [
    Path("."),
    Path("langbridge/packages/common"),
    Path("langbridge/packages/connectors"),
    Path("langbridge/packages/contracts"),
    Path("langbridge/packages/federation"),
    Path("langbridge/packages/messaging"),
    Path("langbridge/packages/orchestrator"),
    Path("langbridge/packages/runtime"),
    Path("langbridge/packages/sdk"),
    Path("langbridge/packages/semantic"),
]
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


def build_packages(version: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="langbridge-package-build-") as temp_dir:
        temp_root = Path(temp_dir) / "langbridge"
        shutil.copytree(REPO_ROOT, temp_root, ignore=IGNORE_PATTERNS)

        for package_dir in PACKAGE_DIRS:
            patch_pyproject(temp_root / package_dir / "pyproject.toml", version)

        for package_dir in PACKAGE_DIRS:
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
