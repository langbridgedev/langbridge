PYTHON ?= python
PIP ?= $(PYTHON) -m pip
NPM ?= npm

.PHONY: help install-dev ui-build clean build build-runtime build-sdk login-codeartifact publish-codeartifact

help:
	@echo "Targets:"
	@echo "  make install-dev"
	@echo "  make ui-build"
	@echo "  make build"
	@echo "  make build-runtime"
	@echo "  make build-sdk"
	@echo "  make login-codeartifact CODEARTIFACT_DOMAIN=... CODEARTIFACT_REPOSITORY=... CODEARTIFACT_DOMAIN_OWNER=..."
	@echo "  make publish-codeartifact CODEARTIFACT_DOMAIN=... CODEARTIFACT_REPOSITORY=... CODEARTIFACT_DOMAIN_OWNER=..."

install-dev:
	$(PIP) install -r requirements/dev.txt
	$(PIP) install -e .

ui-build:
	cd apps/runtime_ui && $(NPM) install && $(NPM) run build

clean:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(path, ignore_errors=True) for path in map(pathlib.Path, ['build', 'dist', 'langbridge.egg-info', 'packages/sdk/build', 'packages/sdk/dist', 'packages/sdk/langbridge_sdk.egg-info'])]"

build: build-runtime build-sdk

build-runtime: clean ui-build
	$(PYTHON) -m build --no-isolation

build-sdk:
	cd packages/sdk && $(PYTHON) -m build --no-isolation
