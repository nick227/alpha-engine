"""Unified Alpha Engine Streamlit shell."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

# Ensure repo root is on sys.path even when invoked as `streamlit run <abs path>`.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Streamlit can import this file as the top-level `app` module when sibling pages
# (for example `dashboard.py`) are launched directly from `app/ui`. When that
# happens, expose the real package path so `app.ui.*` imports still resolve.
if __name__ == "app":
    __path__ = [str(_REPO_ROOT / "app")]

from app.ui.middle.dashboard_service import DashboardService  # noqa: E402
from app.ui.audit import audit_main  # noqa: E402
from app.ui.dashboard_compact import dashboard_compact_main  # noqa: E402
from app.ui.intelligence_hub import intelligence_hub_main  # noqa: E402
from app.ui.discovery import discovery_main  # noqa: E402
from app.ui.explainability_page import explainability_main  # noqa: E402
from app.ui.ops_data_console import ops_data_console_main  # noqa: E402
from app.ui.paper_trades import paper_trades_main  # noqa: E402
from app.ui.shell.filter_state import render_sidebar_filters  # noqa: E402
from app.ui.shell.top_bar import render_top_bar  # noqa: E402
from app.ui.theme import apply_theme  # noqa: E402


@st.cache_resource
def _get_service(db_path: str) -> DashboardService:
    return DashboardService(db_path=db_path)


def main() -> None:
    st.set_page_config(page_title="Alpha Engine", layout="wide", page_icon="ðŸ“ˆ")
    apply_theme()

    db_path = os.environ.get("ALPHA_DB_PATH", "data/alpha.db")
    service = _get_service(db_path)

    filters = render_sidebar_filters(service)
    route = render_top_bar(filters=filters)

    if route == "dashboard":
        dashboard_compact_main(
            service,
            tenant_id=filters["tenant_id"],
            ticker=filters["ticker"],
            horizon_days=filters["horizon_days"],
            show_page_header=False,
        )
        return

    if route == "explain":
        explainability_main(
            service,
            tenant_id=filters["tenant_id"],
            ticker=filters["ticker"],
            show_page_header=False,
        )
        return

    if route == "ih":
        intelligence_hub_main(
            service,
            show_page_header=False,
            show_local_controls=False,
        )
        return
    
    if route == "paper":
        paper_trades_main(
            service,
            tenant_id=filters["tenant_id"],
            ticker=filters["ticker"],
            show_page_header=False,
        )
        return

    if route == "discovery":
        discovery_main(
            service,
            tenant_id=filters["tenant_id"],
            show_page_header=False,
        )
        return
    
    if route == "ops":
        ops_data_console_main(
            service,
            tenant_id=filters["tenant_id"],
            ticker=filters["ticker"],
            show_page_header=False,
        )
        return

    audit_main(
        db_path=db_path,
        show_page_header=False,
        use_sidebar_controls=False,
    )


if __name__ == "__main__":
    main()
