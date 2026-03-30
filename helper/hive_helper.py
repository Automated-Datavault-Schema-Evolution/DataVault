"""Hive / Thrift helper utilities.

Extracted from the original main.py without behavioral changes.
"""

import os

from jinja2 import Template
from logger import log
from config import THRIFT_HOST, THRIFT_PORT


def render_profile_value(value):
    """Render a dbt-style Jinja env_var() expression from profiles.yml."""
    if isinstance(value, str) and "{{" in value:
        return Template(value).render(env_var=lambda name, default=None: os.getenv(name, default))
    return value


def _resolve_thrift(target_cfg):
    """
    Resolve Hive Thrift connection parameters with env taking precedence.

    Drop-in hardening:
      - Supports dbt-style jinja in profiles.yml (e.g. {{ env_var('USER', 'dbt') }})
      - Allows explicit THRIFT_USER override
      - Avoids usernames that do not exist in the container (defaults to 'root' for tests)
    """
    env_host = os.environ.get("THRIFT_HOST")
    env_port = os.environ.get("THRIFT_PORT")
    env_user = os.environ.get("THRIFT_USER")

    host = env_host or target_cfg.get("host") or THRIFT_HOST
    port_raw = env_port or target_cfg.get("port") or THRIFT_PORT
    user = env_user or target_cfg.get("user") or os.environ.get("USER") or "root"

    host = render_profile_value(host)
    port_raw = render_profile_value(port_raw)
    user = render_profile_value(user)

    host = str(host).strip()
    user = str(user).strip()
    port = int(str(port_raw).strip())

    if not user or user == "dbt" or user == "{{ env_var('USER', 'dbt') }}":
        user = "root"

    log.debug(f"Using Hive Thrift server host={host}, port={port}, user={user}")
    return host, port, user


resolve_thrift = _resolve_thrift
