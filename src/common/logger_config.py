import logging
import os


def setup_logging():
    """
    Configure application-wide logging once and reuse handlers.
    """
    log_file = os.getenv("LOG_FILE", "app.log")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Avoid duplicate handlers if called multiple times
    if root_logger.handlers:
        return root_logger

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    logging.info("Logging configured successfully.")
    return root_logger


if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.debug("Debug message test")
    logger.info("Info message test")
    logger.warning("Warning message test")
    logger.error("Error message test")
    logger.critical("Critical message test")
