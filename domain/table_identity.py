"""Pure table naming rules shared by the vault runtime."""

from __future__ import annotations

_PG_IDENTIFIER_MAX = 63



def sanitize_table_name(name: str | None) -> str:
    """Normalize a logical table identifier into a storage-safe name."""
    raw = (name or "").strip()
    if not raw:
        return ""
    if "." in raw:
        raw = raw.split(".", 1)[-1]
    return raw.strip('"').replace('.', '_').replace('-', '_')



def physical_rdbms_table_name(name: str | None) -> str:
    """Return the PostgreSQL-safe physical identifier for a logical table name."""
    return sanitize_table_name(name)[:_PG_IDENTIFIER_MAX]



def physical_table_name(name: str | None, lake_type: str) -> str:
    """Resolve the physical identifier for the configured lake backend."""
    sanitized = sanitize_table_name(name)
    if lake_type == "rdbms":
        return physical_rdbms_table_name(sanitized)
    return sanitized.lower()
