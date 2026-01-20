"""
Zstandard decompression for Reddit data dumps.
"""

import subprocess
import os
from pathlib import Path


def decompress_zst(input_path: str, output_dir: str) -> str:
    """
    Decompress a .zst file using the zstd command-line tool.
    
    Uses a .temp file during decompression and renames to final name on success.
    This ensures partial files from interrupted runs are not mistaken as complete.
    
    Args:
        input_path: Path to the .zst compressed file
        output_dir: Directory to write the decompressed file
        
    Returns:
        Path to the decompressed file
        
    Raises:
        subprocess.CalledProcessError: If decompression fails
        FileNotFoundError: If input file doesn't exist
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Create output directory if needed
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output filename: remove .zst extension
    output_filename = input_path.stem  # e.g., RC_2023-01.zst -> RC_2023-01
    output_path = output_dir / output_filename
    temp_path = output_dir / f"{output_filename}.temp"
    
    # Clean up any leftover temp file from interrupted run
    if temp_path.exists():
        print(f"[DECOMPRESS] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()
    
    print(f"[DECOMPRESS] {input_path.name} -> {output_path}")
    
    # Use zstd to decompress to temp file first
    # -d: decompress
    # -f: force overwrite
    # --long=31: support large window sizes (Reddit dumps use 2GB windows)
    # -o: output file
    cmd = ["zstd", "-d", "-f", "--long=31", str(input_path), "-o", str(temp_path)]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        # Print the actual error from zstd
        if result.stderr:
            print(f"[DECOMPRESS] zstd error: {result.stderr.strip()}")
        if result.stdout:
            print(f"[DECOMPRESS] zstd output: {result.stdout.strip()}")
        # Clean up temp file on failure
        if temp_path.exists():
            temp_path.unlink()
        raise subprocess.CalledProcessError(
            result.returncode, 
            cmd, 
            output=result.stdout, 
            stderr=result.stderr
        )
    
    # Verify temp output exists
    if not temp_path.exists():
        raise RuntimeError(f"Decompression produced no output: {temp_path}")
    
    # Rename temp file to final output path
    temp_path.rename(output_path)
    
    file_size = output_path.stat().st_size
    print(f"[DECOMPRESS] Complete: {output_path.name} ({file_size / (1024**3):.2f} GB)")
    
    return str(output_path)
