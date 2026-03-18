"""
State management for social_data_pipeline pipeline.
Tracks processed files to enable resume capability.
Supports database recovery for postgres_ingest profile.
"""

import json
from pathlib import Path
from typing import Optional
from datetime import datetime


class PipelineState:
    """Manages pipeline state for resume capability."""
    
    def __init__(self, state_file: str = "/data/output/pipeline_state.json", db_config: dict = None, 
                 data_types: list = None, file_prefixes: dict = None):
        """
        Initialize pipeline state manager.
        
        Args:
            state_file: Path to state JSON file
            db_config: Optional database config dict for recovery (postgres_ingest profile)
                      Expected keys: name, user, host, port, schema
            data_types: Optional list of data types (table names) for database recovery
            file_prefixes: Optional dict mapping data_type -> file prefix for recovery
        """
        self.state_file = Path(state_file)
        self.db_config = db_config
        self.data_types = data_types or []
        self.file_prefixes = file_prefixes or {}
        self._load_state()
    
    def _load_state(self):
        """Load state from disk or initialize empty state."""
        # Ensure state directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Try to load existing state file
        if self.state_file.exists() and self.state_file.is_file():
            try:
                with open(self.state_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        self.state = json.loads(content)
                        return
            except (json.JSONDecodeError, IOError) as e:
                print(f"[sdp] Warning: Could not read state file ({e}), starting fresh")
        
        # Initialize empty state
        self.state = {
            "processed": [],
            "failed": [],
            "in_progress": None,
            "last_updated": None
        }
    
    def recover_from_database(self):
        """
        Recover processed files list by querying unique datasets from database tables.
        Only applicable for postgres_ingest profile. Call this after database connection is available.
        """
        if not self.db_config:
            print("[sdp] No database config provided, cannot recover from database")
            return
        
        try:
            import psycopg
        except ImportError:
            print("[sdp] psycopg not available, cannot recover from database")
            return
        
        recovered = []
        
        try:
            connect_kwargs = dict(
                dbname=self.db_config['name'],
                user=self.db_config['user'],
                host=self.db_config['host'],
                port=self.db_config['port'],
            )
            if self.db_config.get('password'):
                connect_kwargs['password'] = self.db_config['password']
            with psycopg.connect(**connect_kwargs) as conn:
                with conn.cursor() as curr:
                    schema = self.db_config.get('schema', 'public')
                    
                    # Helper to check if table exists
                    def table_exists(table_name: str) -> bool:
                        curr.execute("""
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        """, (schema, table_name))
                        return curr.fetchone() is not None
                    
                    # Check each configured data type table
                    for data_type in self.data_types:
                        if table_exists(data_type):
                            curr.execute(f"""
                                SELECT DISTINCT dataset FROM {schema}.{data_type} ORDER BY dataset
                            """)
                            type_count = 0
                            prefix = self.file_prefixes.get(data_type, f"{data_type}_")
                            for row in curr.fetchall():
                                dataset = row[0].strip()  # dataset is char(7), may have trailing space
                                file_id = f"{prefix}{dataset}"
                                if file_id not in recovered:
                                    recovered.append(file_id)
                                    type_count += 1
                            print(f"[sdp] Found {type_count} datasets in {data_type} table")
                        else:
                            print(f"[sdp] {data_type} table does not exist yet")
                        
        except Exception as e:
            print(f"[sdp] Error recovering from database: {e}")
            return
        
        if recovered:
            self.state["processed"] = recovered
            self._save_state()
            print(f"[sdp] Recovered {len(recovered)} processed files from database")
        else:
            print("[sdp] No existing data found in database, starting fresh")
            self._save_state()
    
    def _save_state(self):
        """Persist state to disk."""
        self.state["last_updated"] = datetime.now().isoformat()
        
        # Ensure parent directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def is_processed(self, filename: str) -> bool:
        """Check if a file has already been processed."""
        return filename in self.state["processed"]
    
    def is_failed(self, filename: str) -> bool:
        """Check if a file previously failed processing."""
        return any(f["filename"] == filename for f in self.state["failed"])
    
    def get_in_progress(self) -> Optional[str]:
        """Get the currently in-progress file, if any."""
        return self.state["in_progress"]
    
    def mark_in_progress(self, filename: str):
        """Mark a file as currently being processed."""
        self.state["in_progress"] = filename
        self._save_state()
        print(f"[sdp] Started processing: {filename}")
    
    def mark_completed(self, filename: str):
        """Mark a file as successfully processed."""
        if filename not in self.state["processed"]:
            self.state["processed"].append(filename)
        self.state["in_progress"] = None
        self._save_state()
        print(f"[sdp] Completed: {filename}")
    
    def mark_failed(self, filename: str, error: str):
        """Mark a file as failed with error details."""
        # Remove from failed if already there (to update error)
        self.state["failed"] = [
            f for f in self.state["failed"] 
            if f["filename"] != filename
        ]
        self.state["failed"].append({
            "filename": filename,
            "error": str(error),
            "timestamp": datetime.now().isoformat()
        })
        self.state["in_progress"] = None
        self._save_state()
        print(f"[sdp] Failed: {filename} - {error}")
    
    def clear_in_progress(self):
        """Clear the in-progress marker (e.g., after crash recovery)."""
        if self.state["in_progress"]:
            print(f"[sdp] Clearing stale in-progress: {self.state['in_progress']}")
            self.state["in_progress"] = None
            self._save_state()
    
    def recover_from_mongodb(self):
        """
        Recover processed files list by querying the _sdb_metadata collection in MongoDB.

        Uses the db_config dict which should contain: host, port, db_name, and optionally data_type.
        Works for both per_file and per_data_type collection strategies.
        """
        if not self.db_config:
            print("[sdp] No database config provided, cannot recover from MongoDB")
            return

        try:
            from ..db.mongo.ingest import get_ingested_files
        except ImportError:
            print("[sdp] pymongo not available, cannot recover from MongoDB")
            return

        recovered = []

        try:
            host = self.db_config['host']
            port = self.db_config['port']

            # Query each configured database for metadata
            for data_type in self.data_types:
                db_name = self.db_config.get('db_name_func', lambda dt: dt)(data_type)
                try:
                    file_ids = get_ingested_files(
                        db_name=db_name,
                        host=host,
                        port=port,
                        data_type=data_type,
                        user=self.db_config.get('user'),
                        password=self.db_config.get('password'),
                    )
                    for fid in file_ids:
                        if fid not in recovered:
                            recovered.append(fid)
                    if file_ids:
                        print(f"[sdp] Found {len(file_ids)} ingested files for {data_type} in {db_name}")
                    else:
                        print(f"[sdp] No metadata found for {data_type} in {db_name}")
                except Exception as e:
                    print(f"[sdp] Error querying {db_name} for {data_type}: {e}")

        except Exception as e:
            print(f"[sdp] Error recovering from MongoDB: {e}")
            return

        if recovered:
            self.state["processed"] = recovered
            self._save_state()
            print(f"[sdp] Recovered {len(recovered)} processed files from MongoDB")
        else:
            print("[sdp] No existing metadata found in MongoDB, starting fresh")
            self._save_state()

    def get_stats(self) -> dict:
        """Get processing statistics."""
        return {
            "processed_count": len(self.state["processed"]),
            "failed_count": len(self.state["failed"]),
            "in_progress": self.state["in_progress"],
            "last_updated": self.state["last_updated"]
        }
