"""Shared SaaS connector runtime contracts.

Concrete SaaS connectors are package-owned under ``langbridge-connectors``.
Core ``langbridge`` retains only the shared declarative runtime and helpers.
"""

__all__ = [
    "basic_http",
    "declarative",
]
