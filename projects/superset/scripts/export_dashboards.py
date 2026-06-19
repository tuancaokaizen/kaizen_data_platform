#!/usr/bin/env python3
"""Legacy wrapper — use: python -m superset_deploy export"""

import sys

from superset_deploy.cli import main

if __name__ == "__main__":
    argv = ["export"] + sys.argv[1:]
    raise SystemExit(main(argv))
