import asyncio
import logging

from langbridge.federation.executor import ArtifactCleanupResult, ArtifactStore
from langbridge.runtime.settings import runtime_settings


class RuntimeCleanupService:
    """Runs routine local runtime storage cleanup."""

    def __init__(
        self,
        *,
        federation_artifact_store: ArtifactStore,
        federation_ttl_seconds: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._federation_artifact_store = federation_artifact_store
        self._federation_ttl_seconds = max(
            1,
            int(
                federation_ttl_seconds
                if federation_ttl_seconds is not None
                else runtime_settings.FEDERATION_DEFAULT_TTL_SECONDS
            ),
        )
        self._logger = logger or logging.getLogger("langbridge.runtime.cleanup")

    async def cleanup_resources(self) -> ArtifactCleanupResult:
        result = await asyncio.to_thread(self._cleanup_federation_artifacts)
        if result.errors:
            self._logger.warning(
                "Runtime cleanup completed with %s error(s).",
                len(result.errors),
            )
        self._logger.info(
            "Runtime cleanup removed %s federation manifest(s), %s artifact(s), and %s byte(s).",
            result.deleted_manifests,
            result.deleted_artifacts,
            result.bytes_deleted,
        )
        return result

    def _cleanup_federation_artifacts(self) -> ArtifactCleanupResult:
        return self._federation_artifact_store.cleanup_expired_artifacts(
            ttl_seconds=self._federation_ttl_seconds,
        )
