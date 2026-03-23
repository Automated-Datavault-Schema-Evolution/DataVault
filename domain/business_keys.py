"""Pure heuristics for selecting vault business keys."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List



def _existing_business_keys(models_for_table: Iterable[Dict[str, Any]]) -> list[str]:
    for model in models_for_table:
        keys = [str(value) for value in (model.get("business_keys") or []) if value]
        if keys:
            return keys
    return []



def _candidate_table_tokens(table_name: str) -> list[str]:
    cleaned = str(table_name or "").replace('-', '_')
    parts = [part for part in cleaned.split('_') if part and part not in {"dg", "e2e"}]
    return parts



def infer_business_keys(table_name: str, columns: Iterable[str], models_for_table: Iterable[Dict[str, Any]]) -> List[str]:
    """Infer stable business keys from existing models and observed source columns."""
    existing = _existing_business_keys(models_for_table)
    if existing:
        return existing

    normalized_columns = [str(column).strip().lower() for column in columns if str(column).strip()]
    if not normalized_columns:
        return ["id"]

    preferred: list[str] = []
    for token in _candidate_table_tokens(table_name):
        singular = token[:-1] if token.endswith('s') else token
        preferred.extend([f"{singular}id", f"{singular}_id"])
    preferred.extend(["id", "customerid", "customer_id", "accountid", "account_id", "orderid", "order_id"])

    selected = [candidate for candidate in preferred if candidate in normalized_columns]
    if selected:
        return [selected[0]]

    suffix_matches = [column for column in normalized_columns if column.endswith('id') or column.endswith('_id')]
    if suffix_matches:
        return [suffix_matches[0]]

    return [normalized_columns[0]]
