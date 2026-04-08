import logging
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_logger(name: str, log_dir: str = os.getenv("LOG_DIR", "logs")) -> logging.Logger:
    """
    Crée et retourne un logger configuré avec :
    - affichage console (StreamHandler)
    - écriture fichier horodaté (FileHandler)

    Args:
        name     : nom du module appelant (ex: 'ingestion')
        log_dir  : dossier où écrire les fichiers de log

    Returns:
        logging.Logger configuré
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # Evite les doublons si le logger est déjà initialisé
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ── Format ────────────────────────────────────────────
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Handler console ───────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    # ── Handler fichier (un fichier par jour et par module) ──
    log_filename = f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(
        filename=os.path.join(log_dir, log_filename),
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger