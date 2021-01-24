import logging
import logging.handlers
import os

log_level = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "CRITICAL": logging.CRITICAL,
}


class LogHandler:
    def __init__(self):
        pass

    def create_logger(
        self,
        logger_name,
        console=True,
        console_level="DEBUG",
        file=True,
        file_level="DEBUG",
        log_directory="logs/",
        log_format="%(asctime)s | %(message)s",
        date_format="%m/%d/%Y %I:%M:%S%p",
    ):
        if not log_directory.endswith("/"):
            log_directory = f"{log_directory}/"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_file = f"{log_directory}{logger_name}.log"

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

        if console:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(log_level[console_level])
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        if file:
            rotating_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=1e7, backupCount=10
            )
            rotating_handler.setLevel(log_level[file_level])
            rotating_handler.setFormatter(formatter)
            logger.addHandler(rotating_handler)

        return logger