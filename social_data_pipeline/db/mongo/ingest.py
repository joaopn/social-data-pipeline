"""
MongoDB operations for social_data_pipeline.

Provides collection management, mongoimport-based bulk ingestion,
index creation, and metadata tracking for the mongo_ingest profile.

Adapted from db_tools/db_tools/mongodb.py with project-consistent conventions.
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional


def get_mongo_uri(host: str, port: int, user: str = None, password: str = None) -> str:
    """Build MongoDB connection URI. Uses authSource=admin when credentials are provided."""
    if user and password:
        from urllib.parse import quote_plus
        return f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/?authSource=admin"
    if user:
        from urllib.parse import quote_plus
        return f"mongodb://{quote_plus(user)}:@{host}:{port}/?authSource=admin"
    return f"mongodb://{host}:{port}"


def _get_client(host: str, port: int, user: str = None, password: str = None):
    """Lazy-import pymongo and return a MongoClient."""
    from pymongo import MongoClient
    return MongoClient(get_mongo_uri(host, port, user, password))


def ensure_collection(
    db_name: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> bool:
    """
    Ensure a collection exists with zstd WiredTiger compression.

    Returns True if the collection was newly created, False if it already existed.
    """
    client = _get_client(host, port, user, password)
    try:
        if collection_name in client[db_name].list_collection_names():
            return False

        client[db_name].create_collection(
            collection_name,
            storageEngine={
                'wiredTiger': {
                    'configString': 'block_compressor=zstd',
                }
            },
        )
        print(f"[sdp] Created collection {db_name}.{collection_name} (zstd)")
        return True
    finally:
        client.close()


def _redact_uri(uri: str) -> str:
    """Replace password in a MongoDB URI with ***."""
    import re
    return re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', uri)


def mongoimport_file(
    filepath: str,
    db_name: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
    num_workers: int = 4,
    log_dir: str = "/data/mongo/logs",
) -> None:
    """
    Ingest a file using mongoimport subprocess.

    Supports JSON/NDJSON (default) and CSV (auto-detected from file extension).
    Logs are appended to {log_dir}/mongoimport_{db}_{collection}.log.
    Raises RuntimeError on failure (never exposes credentials in exceptions).
    """
    uri = get_mongo_uri(host, port, user, password)
    command = [
        "mongoimport",
        f"--uri={uri}",
        "--db", db_name,
        "--collection", collection_name,
        "--file", filepath,
        "--numInsertionWorkers", str(num_workers),
    ]

    # Auto-detect CSV input from file extension
    if filepath.lower().endswith('.csv'):
        command.extend(["--type", "csv", "--headerline"])

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"mongoimport_{db_name}_{collection_name}.log")

    # Build redacted command for logging (never write credentials to logs)
    redacted_command = [_redact_uri(arg) if '://' in arg else arg for arg in command]

    with open(log_file, 'a') as log:
        log.write(f"\n{'='*60}\n")
        log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        log.write(f"Command: {' '.join(redacted_command)}\n")
        log.write(f"File: {filepath}\n")
        log.write(f"{'='*60}\n")
        result = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )

    if result.returncode != 0:
        raise RuntimeError(
            f"mongoimport failed for {filepath} -> {db_name}.{collection_name}. "
            f"See log: {log_file}"
        )


def create_index(
    db_name: str,
    collection_name: str,
    field: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> None:
    """Create a single ascending index on a collection."""
    from pymongo import ASCENDING

    client = _get_client(host, port, user, password)
    try:
        index_name = f"{field}_1"
        existing = client[db_name][collection_name].list_indexes()
        existing_names = [idx['name'] for idx in existing]

        if index_name in existing_names:
            print(f"[sdp] Index {index_name} already exists on {db_name}.{collection_name}")
            return

        client[db_name][collection_name].create_index(
            [(field, ASCENDING)],
            name=index_name,
        )
        print(f"[sdp] Created index {index_name} on {db_name}.{collection_name}")
    finally:
        client.close()


def get_collection_names(db_name: str, host: str, port: int, user: str = None, password: str = None) -> List[str]:
    """Get list of collection names in a database (excludes system/metadata collections)."""
    client = _get_client(host, port, user, password)
    try:
        names = client[db_name].list_collection_names()
        return [n for n in sorted(names) if not n.startswith('_')]
    finally:
        client.close()


def record_ingested_file(
    db_name: str,
    file_id: str,
    data_type: str,
    collection_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
) -> None:
    """
    Record an ingested file in the _sdb_metadata collection.

    Stores file_id, data_type, collection_name, and timestamp.
    Used for state recovery when the state JSON file is lost.
    """
    client = _get_client(host, port, user, password)
    try:
        metadata = client[db_name]['_sdb_metadata']
        metadata.update_one(
            {'file_id': file_id, 'data_type': data_type},
            {
                '$set': {
                    'file_id': file_id,
                    'data_type': data_type,
                    'collection': collection_name,
                    'ingested_at': datetime.now().isoformat(),
                }
            },
            upsert=True,
        )
    finally:
        client.close()


def get_ingested_files(
    db_name: str,
    host: str,
    port: int,
    user: str = None,
    password: str = None,
    data_type: Optional[str] = None,
) -> List[str]:
    """
    Get list of ingested file_ids from the _sdb_metadata collection.

    Args:
        db_name: MongoDB database name
        host: MongoDB host
        port: MongoDB port
        data_type: If provided, filter by data_type

    Returns:
        List of file_id strings
    """
    client = _get_client(host, port, user, password)
    try:
        metadata = client[db_name]['_sdb_metadata']
        query = {'data_type': data_type} if data_type else {}
        return [doc['file_id'] for doc in metadata.find(query, {'file_id': 1, '_id': 0})]
    finally:
        client.close()
