"""
Reddit Data Tools - Unified pipeline for Reddit data processing.

Profiles:
- parse: Extract and parse .zst dumps to CSV
- ml_cpu: Run Lingua language detection (CPU only)
- ml: Run GPU-based transformer classifiers
- postgres: Run PostgreSQL database server
- postgres_ingest: Ingest base data into PostgreSQL
- postgres_ml: Ingest ML classifier outputs into PostgreSQL sidecars
"""

__version__ = "1.0.0"
