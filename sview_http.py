#!/usr/bin/env python3
"""CLI entry. Implementation: ``sview.http.server``."""
from pathlib import Path
import runpy
import sys

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

if __name__ == "__main__":
    runpy.run_module("sview.http.server", run_name="__main__")
