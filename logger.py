import logging
import re
import sys

from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

from env import env


def setup_logging(project_root: Path):
    """
    Set up logging with stdout and daily log rotation in {project_root}/log/YYYYMMDD.log

    :param project_root: Path to the root of the project
    """

    log_dir = project_root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{env.get_app__name().lower()}.log"

    logger = logging.getLogger(env.get_app__name())
    logger.setLevel(getattr(logging, env.get_app__log_level(), logging.DEBUG))

    logger.propagate = False  # Prevent double logging if root logger is also configured

    log_format = logging.Formatter(
        fmt="%(asctime)sZ [%(name)s] [%(levelname)s] [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    # Timed rotating file handler (rotate at midnight)
    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", utc=True, backupCount=7
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(getattr(logging, env.get_app__log_level(), logging.DEBUG))
    file_handler.suffix = "%Y%m%d"
    file_handler.extMatch = re.compile(r"^\d{8}$")
    logger.addHandler(file_handler)

    # Stream (stdout) handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_format)
    stream_handler.setLevel(getattr(logging, env.get_app__log_level(), logging.INFO))
    logger.addHandler(stream_handler)

    logger.debug("Logger initialized")
