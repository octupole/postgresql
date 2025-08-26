#!/usr/bin/env python3
"""
CSV Importer - Library-based version

This script provides the same CLI interface as the original csv_importer.py
but uses the new PGTools library internally. It maintains full backward
compatibility while providing access to the improved functionality.
"""

from pgtools.cli.csv_importer_cli import main

if __name__ == "__main__":
    main()