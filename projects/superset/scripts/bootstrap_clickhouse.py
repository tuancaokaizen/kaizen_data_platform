#!/usr/bin/env python3
"""Legacy wrapper — use: python -m superset_deploy bootstrap"""

import argparse
import sys

from superset_deploy.cli import cmd_bootstrap

if __name__ == "__main__":
    sys.exit(cmd_bootstrap(argparse.Namespace()))
