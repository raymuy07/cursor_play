import logging
import sys

from app.common.utils import load_config

logger = logging.getLogger(__name__)


class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int) -> None:
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level

def setup_logging():
    """Configure root logger - call once at service startup."""
    log_config = {}
    try:

        ##TODO: we need to refactor that, why logging depends on config?
        config = load_config()
        log_config = config.get("logging", {})
    except Exception:
        pass

    root_logger = logging.getLogger()  # ROOT logger
    root_logger.setLevel(getattr(logging, log_config.get("level", "INFO")))

    ## !TODO,I defiently need to change looging structure into json.
    if not root_logger.handlers:
        formatter = logging.Formatter(log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
