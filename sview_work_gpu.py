#!/usr/bin/env python3
"""CLI entry. Implementation: ``sview.worker.gpu``."""
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sview.worker.gpu import main

if __name__ == "__main__":
    main()
