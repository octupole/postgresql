#!/usr/bin/env python3
"""
Schema Generator - Library-based version

This script provides the same CLI interface as the original schema_generator.py
but uses the new PGTools library internally. It maintains full backward
compatibility while providing access to the improved functionality.
"""

from pgtools.cli.schema_generator_cli import main

if __name__ == "__main__":
    main()