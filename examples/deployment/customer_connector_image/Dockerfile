# syntax=docker/dockerfile:1

# Build a customer-specific Langbridge runtime image with only the connector
# packages this deployment needs.
#
# Build from the repository root:
#
#   docker build \
#     -f examples/deployment/customer_connector_image/Dockerfile \
#     --build-arg LANGBRIDGE_IMAGE=ghcr.io/langbridgedev/langbridge:latest \
#     -t my-company/langbridge-runtime:connectors \
#     .
#
ARG LANGBRIDGE_IMAGE=ghcr.io/langbridgedev/langbridge:latest

FROM ${LANGBRIDGE_IMAGE}

COPY examples/deployment/customer_connector_image/requirements-connectors.txt /tmp/requirements-connectors.txt

# The current base image contains the runtime source under /app. Install it as a
# package in the derived image so connector packages that depend on `langbridge`
# resolve against this embedded runtime instead of pulling another copy.
RUN python -m pip install --no-cache-dir --no-deps --no-build-isolation /app

RUN if grep -Eq '^[[:space:]]*[^#[:space:]]' /tmp/requirements-connectors.txt; then \
      python -m pip install --no-cache-dir --requirement /tmp/requirements-connectors.txt; \
    else \
      echo "No connector packages configured in requirements-connectors.txt"; \
    fi \
    && rm /tmp/requirements-connectors.txt
