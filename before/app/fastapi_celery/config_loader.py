import os
from pathlib import Path
from dotenv import load_dotenv
from configparser import ConfigParser

# Load .env file (useful for local dev)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env")

# Load configs.ini
config = ConfigParser()
config.read(Path(__file__).resolve().parent / "configs.ini")

# Shortcut accessors
def get_config_value(section: str, key: str, fallback=None):
    return config.get(section, key, fallback=fallback)

def get_env_variable(key: str, fallback=None):
    """
    Get an environment variable.
    """
    return os.getenv(key, fallback)
