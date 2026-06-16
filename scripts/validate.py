#!/usr/bin/env python3
"""Wrapper for ``python -m datahub.validation``."""

from datahub.validation.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
