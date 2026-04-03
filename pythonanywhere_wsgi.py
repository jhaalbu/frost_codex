import os
import sys
from pathlib import Path


PROJECT_HOME = Path("/home/yourusername/frost_codex")
if str(PROJECT_HOME) not in sys.path:
    sys.path.insert(0, str(PROJECT_HOME))

# Optionally point to a dedicated production env file on PythonAnywhere.
os.environ.setdefault("FROST_ENV_FILE", str(PROJECT_HOME / ".env"))

from app import app as application
