#!/usr/bin/env python3
"""Legacy wrapper — use: python -m superset_deploy deploy"""

import sys

from superset_deploy.cli import main

if __name__ == "__main__":
    # Forward legacy flags to deploy subcommand / Chuyển flags cũ sang subcommand deploy
    argv = ["deploy"] + sys.argv[1:]
    raise SystemExit(main(argv))
