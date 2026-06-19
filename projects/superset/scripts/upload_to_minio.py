#!/usr/bin/env python3
"""Legacy wrapper — use: python -m superset_deploy sync-minio"""

import sys

from superset_deploy.cli import main

if __name__ == "__main__":
    argv = ["sync-minio", "--sync-all"] + sys.argv[1:]
    raise SystemExit(main(argv))
