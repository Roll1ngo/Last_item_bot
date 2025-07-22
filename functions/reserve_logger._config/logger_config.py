import logging.handlers
import logging
import os
import sys
import threading

import coloredlogs

from functions.load_config import get_config

config = get_config()
if config:
    log_colors = config.get('log_colors')

    info = log_colors.get('info')  # для тесту info
    warning = log_colors.get('warning')  # для виділених рядків
    time = log_colors.get('time')  # для часу
    level = log_colors.get('level')  # для рівня
    module = log_colors.get('module')  # для модуля
    funcName = log_colors.get('funcName')  # для функції
    linenumber = log_colors.get('linenumber')  # для номера рядка

else:
    print("Помилка завантаження конфігурації.")

max_bytes = 1024 * 1024 * 100
backup_count = 1

# Create a logger instance
logger = logging.getLogger(__name__)

# Set the logging level to DEBUG to capture all messages
logger.setLevel(logging.INFO)

script_path = os.path.abspath(__file__)
parent_directory = os.path.dirname(os.path.dirname(script_path))
log_file_path = os.path.join(parent_directory, "item_bot.ans")


# Create a file handler using the `logging.handlers` module
file_handler = logging.handlers.RotatingFileHandler(
    log_file_path,
    encoding="utf-8",
    maxBytes=max_bytes,
    backupCount=backup_count
)

# Formatter for the file
file_formatter = logging.Formatter(
    '\033[38;5;214m%(asctime)s - \033[38;5;226m%(levelname)s -'  # Yellow-orange for time and level
        ' %(module)s.\033[32m%(funcName)s\033[38;5;214m:%(lineno)d -'  # Green for functions
        ' \033[38;5;75m%(message)s\033[0m',)
file_handler.setFormatter(file_formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Enable UTF-8 encoding for console output
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

# Налаштування логування
coloredlogs.install(
    level='INFO',
    fmt='%(asctime)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%H:%M:%S',
    level_styles={
        'info': {'color': info},  # Жовтий для INFO
        'warning': {'color': warning},  # Синій для WARNING
    },
    field_styles={
        'asctime': {'color': time},  # Білий для часу
        'levelname': {'color': level},  # Білий для рівня
        'module': {'color': module},  # Білий для модуля
        'funcName': {'color': funcName},  # Зелений для функції
        'lineno': {'color': linenumber},  # Білий для номера рядка
    }
)


# Logging uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Log all uncaught exceptions as critical errors with full traceback
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


# Override default exception hook
sys.excepthook = handle_exception


# Custom exception handler for threads
def thread_exception_handler(args):
    logger.critical(f"Exception in thread {args.thread.name} ({args.thread.ident})", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


# Set the custom exception handler for threads
threading.excepthook = thread_exception_handler


