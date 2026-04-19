"""Optional Azure Application Insights via OpenTelemetry (Azure plugin guidance)."""

from __future__ import annotations

import logging
import os

_LOGGER_NAMESPACE = "stock_analysis_app"
_configured = False
_logger: logging.Logger | None = None


def setup_azure_monitor() -> logging.Logger | None:
    """
    Send logs and traces to Application Insights when
    APPLICATIONINSIGHTS_CONNECTION_STRING is set.

    No-op when unset or when azure-monitor-opentelemetry is not installed.
    """
    global _configured, _logger
    if _configured:
        return _logger

    _configured = True
    if not os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "").strip():
        return None

    try:
        from azure.monitor.opentelemetry import configure_azure_monitor
    except ImportError:
        return None

    configure_azure_monitor(logger_name=_LOGGER_NAMESPACE)
    log = logging.getLogger(_LOGGER_NAMESPACE)
    log.setLevel(logging.INFO)
    _logger = log
    log.info("Azure Monitor telemetry enabled for stock-analysis-app.")
    return _logger


def get_telemetry_logger() -> logging.Logger | None:
    """Return the Application Insights-backed logger if configured."""
    return _logger
