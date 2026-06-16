"""Module execution entry point for ``python -m datahub.validation``."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
