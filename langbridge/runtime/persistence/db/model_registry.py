
from .base import Base


def register_runtime_metadata_models() -> None:
    # Importing the model modules registers every runtime metadata table against Base.metadata.
    from . import agent  # noqa: F401
    from . import auth  # noqa: F401
    from . import connector  # noqa: F401
    from . import connector_sync  # noqa: F401
    from . import dataset  # noqa: F401
    from . import job  # noqa: F401
    from . import lineage  # noqa: F401
    from . import runtime  # noqa: F401
    from . import semantic  # noqa: F401
    from . import sql  # noqa: F401
    from . import threads  # noqa: F401
    from . import workspace  # noqa: F401


def get_runtime_metadata():
    register_runtime_metadata_models()
    return Base.metadata


__all__ = [
    "get_runtime_metadata",
    "register_runtime_metadata_models",
]
