#!/usr/bin/env python3
"""命令行入口。实现：``sview.worker.cpu``。"""
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sview.worker.cpu import main

if __name__ == "__main__":
    main()
