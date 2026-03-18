# langbridge-connectors

Connector adapters and schema helpers published from the Langbridge runtime
repository for cloud and self-hosted consumers.

The connector protocols, factories, and plugin registration surface now have a
canonical runtime entrypoint at `langbridge.plugins`. Official connector
implementations remain in this separate `langbridge-connectors` package and
register through that core plugin surface.
