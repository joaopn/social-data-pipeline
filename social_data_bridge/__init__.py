"""
Social Data Bridge - Unified pipeline for social media data processing.

Supports multiple platforms via the PLATFORM environment variable.

Profiles:
- parse: Extract and parse data dumps to CSV
- lingua: Run Lingua language detection (CPU only)
- ml: Run GPU-based transformer classifiers
- postgres: Run PostgreSQL database server
- postgres_ingest: Ingest base data into PostgreSQL
- postgres_ml: Ingest ML classifier outputs into PostgreSQL
"""

__version__ = "2.0.0"
