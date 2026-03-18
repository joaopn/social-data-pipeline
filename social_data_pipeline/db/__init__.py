"""
Database module for social_data_pipeline.

Submodules (postgres, mongo) are imported directly where needed,
not eagerly here, to avoid pulling in dependencies that may not
be installed in every Docker image.
"""
