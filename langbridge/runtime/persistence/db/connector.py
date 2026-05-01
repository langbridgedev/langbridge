from sqlalchemy import Uuid as UUID, Boolean, Column, ForeignKey, JSON, String

from .base import Base


class Connector(Base):
    __tablename__ = "connectors"

    id = Column(UUID(as_uuid=True), primary_key=True, index=True)
    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(String(1024))
    connector_type = Column(String(50), nullable=False)
    connector_family = Column(String(50), nullable=True)
    config_json = Column(String, nullable=False)
    connection_metadata_json = Column(JSON, nullable=True)
    secret_references_json = Column(JSON, nullable=True)
    access_policy_json = Column(JSON, nullable=True)
    supported_resources_json = Column(JSON, nullable=True)
    default_sync_strategy = Column("sync_strategy", String(50), nullable=True)
    capabilities_json = Column(JSON, nullable=True)
    is_managed = Column(Boolean, default=False, nullable=False)
    created_by_actor_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    updated_by_actor_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    management_mode = Column(String(50), nullable=False)
    lifecycle_state = Column(String(50), nullable=False)
