# Customer Connector Image Example

This example shows the recommended production pattern for keeping the official
Langbridge Docker image lean while allowing customers to install only the
connector packages their runtime needs.

The base Langbridge image should contain the portable runtime and core
dependencies. Customer deployments should derive from that image and install
pinned connector packages at build time.

## Files

- `Dockerfile`
  Builds a customer-specific runtime image from the official Langbridge image.
- `requirements-connectors.txt`
  Lists the connector packages to install into the derived image.

## Build

From the repository root:

```bash
docker build \
  -f examples/deployment/customer_connector_image/Dockerfile \
  --build-arg LANGBRIDGE_IMAGE=ghcr.io/langbridgedev/langbridge:latest \
  -t my-company/langbridge-runtime:connectors \
  .
```

For a production deployment, pin both the base image and connector packages:

```bash
docker build \
  -f examples/deployment/customer_connector_image/Dockerfile \
  --build-arg LANGBRIDGE_IMAGE=ghcr.io/langbridgedev/langbridge:0.1.0 \
  -t my-company/langbridge-runtime:0.1.0-hubspot \
  .
```

## Configure Connectors

Edit `requirements-connectors.txt` and include only the packages needed by the
runtime deployment. This example installs the HubSpot connector:

```text
langbridge-connector-hubspot==0.1.0
```

Connector packages register themselves through the `langbridge.connectors`
Python entry-point group. Once installed in the image, Langbridge discovers
them at runtime through the standard plugin registry.

The Dockerfile installs the runtime source already present at `/app` as a
Python package before installing connector packages. That keeps connector
package dependencies pointed at the embedded runtime version instead of pulling
a second `langbridge` package from an index.

## Validate The Image

List discovered connector plugin types:

```bash
docker run --rm \
  --entrypoint python \
  my-company/langbridge-runtime:connectors \
  -c "from langbridge.plugins import list_connector_plugins; print([p.connector_type.value for p in list_connector_plugins()])"
```

## Use The Image

Use the derived image anywhere the base runtime image is used:

```yaml
services:
  runtime-host:
    image: my-company/langbridge-runtime:0.1.0-hubspot
    command:
      - serve
      - --config
      - /config/langbridge_config.yml
      - --host
      - 0.0.0.0
      - --port
      - "8000"
    volumes:
      - ./langbridge_config.yml:/config/langbridge_config.yml:ro
```

## Why This Pattern

- Keeps the official runtime image small.
- Keeps customer deployments reproducible.
- Lets security teams review and pin connector dependencies.
- Avoids installing packages inside a running container.
- Works with public or private Python package indexes.

For private package indexes, configure pip during the build with your normal CI
secret mechanism instead of baking credentials into the Dockerfile.
