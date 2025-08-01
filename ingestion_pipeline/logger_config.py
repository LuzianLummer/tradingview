import logging
import os


def setup_logging():
    """
    Konfiguriert das Logging für die gesamte Anwendung.
    """
    log_file = os.getenv("LOG_FILE", "app.log")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Erstelle den Logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Erstelle Console Handler und setze Level auf DEBUG
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Erstelle File Handler und setze Level auf INFO
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Erstelle Formatter und füge ihn den Handlern hinzu
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Füge Handler zum Logger hinzu
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logging.info("Logging configured successfully.")


if __name__ == "__main__":
    setup_logging()
    # Beispiel-Logs, um die Konfiguration zu testen
    logger = logging.getLogger(__name__)
    logger.debug("Dies ist eine DEBUG-Nachricht")
    logger.info("Dies ist eine INFO-Nachricht")
    logger.warning("Dies ist eine WARNING-Nachricht")
    logger.error("Dies ist eine ERROR-Nachricht")
    logger.critical("Dies ist eine CRITICAL-Nachricht")
