"""
State management for reddit_data_tools pipeline.
Tracks processed files to enable resume capability.
Supports database recovery for postgres_ingest profile.
"""

import json
from pathlib import Path
from typing import Optional
from datetime import datetime


class PipelineState:
    """Manages pipeline state for resume capability."""
    
    def __init__(self, state_file: str = "/data/output/pipeline_state.json", db_config: dict = None):
        """
        Initialize pipeline state manager.
        
        Args:
            state_file: Path to state JSON file
            db_config: Optional database config dict for recovery (postgres_ingest profile)
                      Expected keys: name, user, host, port, schema
        """
        self.state_file = Path(state_file)
        self.db_config = db_config
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
                print(f"[STATE] Warning: Could not read state file ({e}), starting fresh")
        
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
            print("[STATE] No database config provided, cannot recover from database")
            return
        
        try:
            import psycopg
        except ImportError:
            print("[STATE] psycopg not available, cannot recover from database")
            return
        
        recovered = []
        
        try:
            with psycopg.connect(
                dbname=self.db_config['name'],
                user=self.db_config['user'],
                host=self.db_config['host'],
                port=self.db_config['port']
            ) as conn:
                with conn.cursor() as curr:
                    schema = self.db_config.get('schema', 'public')
                    
                    # Helper to check if table exists
                    def table_exists(table_name: str) -> bool:
                        curr.execute("""
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        """, (schema, table_name))
                        return curr.fetchone() is not None
                    
                    # Check submissions table
                    if table_exists('submissions_base'):
                        curr.execute(f"""
                            SELECT DISTINCT dataset FROM {schema}.submissions_base ORDER BY dataset
                        """)
                        for row in curr.fetchall():
                            dataset = row[0].strip()  # dataset is char(7), may have trailing space
                            file_id = f"RS_{dataset}"
                            if file_id not in recovered:
                                recovered.append(file_id)
                        print(f"[STATE] Found {len(recovered)} datasets in submissions table")
                    else:
                        print("[STATE] Submissions table does not exist yet")
                    
                    # Check comments table
                    if table_exists('comments_base'):
                        curr.execute(f"""
                            SELECT DISTINCT dataset FROM {schema}.comments_base ORDER BY dataset
                        """)
                        comments_count = 0
                        for row in curr.fetchall():
                            dataset = row[0].strip()
                            file_id = f"RC_{dataset}"
                            if file_id not in recovered:
                                recovered.append(file_id)
                                comments_count += 1
                        print(f"[STATE] Found {comments_count} datasets in comments table")
                    else:
                        print("[STATE] Comments table does not exist yet")
                        
        except Exception as e:
            print(f"[STATE] Error recovering from database: {e}")
            return
        
        if recovered:
            self.state["processed"] = recovered
            self._save_state()
            print(f"[STATE] Recovered {len(recovered)} processed files from database")
        else:
            print("[STATE] No existing data found in database, starting fresh")
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
        print(f"[STATE] Started processing: {filename}")
    
    def mark_completed(self, filename: str):
        """Mark a file as successfully processed."""
        if filename not in self.state["processed"]:
            self.state["processed"].append(filename)
        self.state["in_progress"] = None
        self._save_state()
        print(f"[STATE] Completed: {filename}")
    
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
        print(f"[STATE] Failed: {filename} - {error}")
    
    def clear_in_progress(self):
        """Clear the in-progress marker (e.g., after crash recovery)."""
        if self.state["in_progress"]:
            print(f"[STATE] Clearing stale in-progress: {self.state['in_progress']}")
            self.state["in_progress"] = None
            self._save_state()
    
    def get_stats(self) -> dict:
        """Get processing statistics."""
        return {
            "processed_count": len(self.state["processed"]),
            "failed_count": len(self.state["failed"]),
            "in_progress": self.state["in_progress"],
            "last_updated": self.state["last_updated"]
        }
