"""
Reddit Data Tools - Unified pipeline for Reddit data processing.

Profiles:
- parse: Extract and parse .zst dumps to CSV
- ml_cpu: Run Lingua language detection (CPU only)
- ml: Run GPU-based transformer classifiers
- db_pg: Ingest data into PostgreSQL
"""

__version__ = "1.0.0"
