"""
Decompression for compressed data dumps.

Supports zstandard (.zst), gzip (.gz, .json.gz), xz (.xz), and tar+gzip (.tar.gz, .tgz).
All functions use atomic temp-file writes: decompress to .temp, rename on success.
"""

import subprocess
import os
from pathlib import Path


# ============================================================================
# Compression format detection
# ============================================================================

# Recognized compressed extensions, ordered longest-first for matching
COMPRESSED_EXTENSIONS = ['.tar.gz', '.tgz', '.json.gz', '.zst', '.gz', '.xz']


def detect_compression(filename: str) -> str | None:
    """Detect compression format from filename extension.

    Returns:
        Compression type string ('zst', 'gz', 'xz', 'tar.gz') or None if unrecognized.
    """
    name = filename.lower()
    if name.endswith('.tar.gz') or name.endswith('.tgz'):
        return 'tar.gz'
    if name.endswith('.zst'):
        return 'zst'
    if name.endswith('.json.gz') or name.endswith('.gz'):
        return 'gz'
    if name.endswith('.xz'):
        return 'xz'
    return None


def strip_compression_extension(filename: str) -> str:
    """Remove the compression extension from a filename.

    E.g., 'data.json.gz' -> 'data.json', 'data.zst' -> 'data',
    'archive.tar.gz' -> 'archive'.
    """
    name = filename
    lower = name.lower()
    if lower.endswith('.tar.gz'):
        return name[:-7]
    if lower.endswith('.tgz'):
        return name[:-4]
    if lower.endswith('.json.gz'):
        return name[:-3]  # Keep .json, remove .gz -> 'data.json'
    for ext in ('.zst', '.gz', '.xz'):
        if lower.endswith(ext):
            return name[:-len(ext)]
    return name


def is_compressed(filename: str) -> bool:
    """Check if a filename has a recognized compression extension."""
    return detect_compression(filename) is not None


# ============================================================================
# Unified decompression
# ============================================================================

def decompress_file(input_path: str, output_dir: str) -> str:
    """Decompress a file, auto-detecting format from extension.

    Routes to the appropriate decompressor based on file extension.

    Args:
        input_path: Path to the compressed file
        output_dir: Directory to write the decompressed file

    Returns:
        Path to the decompressed file

    Raises:
        ValueError: If compression format is unrecognized
        subprocess.CalledProcessError: If decompression fails
        FileNotFoundError: If input file doesn't exist
    """
    fmt = detect_compression(str(input_path))
    if fmt is None:
        raise ValueError(f"Unrecognized compression format: {input_path}")

    if fmt == 'zst':
        return decompress_zst(input_path, output_dir)
    elif fmt == 'gz':
        return decompress_gz(input_path, output_dir)
    elif fmt == 'xz':
        return decompress_xz(input_path, output_dir)
    elif fmt == 'tar.gz':
        return decompress_tar_gz(input_path, output_dir)
    else:
        raise ValueError(f"Unsupported compression format: {fmt}")


# ============================================================================
# Zstandard (.zst)
# ============================================================================

def decompress_zst(input_path: str, output_dir: str) -> str:
    """Decompress a .zst file using the zstd command-line tool.

    Uses a .temp file during decompression and renames to final name on success.

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

    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = input_path.stem  # RC_2023-01.zst -> RC_2023-01
    output_path = output_dir / output_filename
    temp_path = output_dir / f"{output_filename}.temp"

    if temp_path.exists():
        print(f"[sdp] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()

    print(f"[sdp] Decompressing {input_path.name}")

    # --long=31: support large window sizes (Reddit dumps use 2GB windows)
    cmd = ["zstd", "-d", "-f", "--long=31", str(input_path), "-o", str(temp_path)]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        if result.stderr:
            print(f"[sdp] zstd error: {result.stderr.strip()}")
        if result.stdout:
            print(f"[sdp] zstd output: {result.stdout.strip()}")
        if temp_path.exists():
            temp_path.unlink()
        raise subprocess.CalledProcessError(
            result.returncode, cmd, output=result.stdout, stderr=result.stderr
        )

    if not temp_path.exists():
        raise RuntimeError(f"Decompression produced no output: {temp_path}")

    temp_path.rename(output_path)

    file_size = output_path.stat().st_size
    print(f"[sdp] Decompressed: {output_path.name} ({file_size / (1024**3):.2f} GB)")

    return str(output_path)


# ============================================================================
# Gzip (.gz, .json.gz)
# ============================================================================

def decompress_gz(input_path: str, output_dir: str) -> str:
    """Decompress a .gz / .json.gz file using gzip CLI.

    Args:
        input_path: Path to the gzip compressed file
        output_dir: Directory to write the decompressed file

    Returns:
        Path to the decompressed file
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # .json.gz -> .json (strip only .gz); .gz -> stem
    output_filename = strip_compression_extension(input_path.name)
    # For .json.gz, output_filename is 'data.json' — strip .json too for consistency
    # since extracted files are identified by stem (no extension)
    if output_filename.endswith('.json'):
        output_filename = output_filename[:-5]

    output_path = output_dir / output_filename
    temp_path = output_dir / f"{output_filename}.temp"

    if temp_path.exists():
        print(f"[sdp] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()

    print(f"[sdp] Decompressing {input_path.name}")

    # gzip -d -c: decompress to stdout, redirect to temp file
    cmd = ["gzip", "-d", "-c", str(input_path)]
    with open(temp_path, 'wb') as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=False)

    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace') if result.stderr else ''
        if stderr:
            print(f"[sdp] gzip error: {stderr.strip()}")
        if temp_path.exists():
            temp_path.unlink()
        raise subprocess.CalledProcessError(
            result.returncode, cmd, stderr=stderr
        )

    if not temp_path.exists():
        raise RuntimeError(f"Decompression produced no output: {temp_path}")

    temp_path.rename(output_path)

    file_size = output_path.stat().st_size
    print(f"[sdp] Decompressed: {output_path.name} ({file_size / (1024**3):.2f} GB)")

    return str(output_path)


# ============================================================================
# XZ (.xz)
# ============================================================================

def decompress_xz(input_path: str, output_dir: str) -> str:
    """Decompress a .xz file using xz CLI.

    Args:
        input_path: Path to the xz compressed file
        output_dir: Directory to write the decompressed file

    Returns:
        Path to the decompressed file
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = input_path.stem  # data.xz -> data
    output_path = output_dir / output_filename
    temp_path = output_dir / f"{output_filename}.temp"

    if temp_path.exists():
        print(f"[sdp] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()

    print(f"[sdp] Decompressing {input_path.name}")

    cmd = ["xz", "-d", "-c", str(input_path)]
    with open(temp_path, 'wb') as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=False)

    if result.returncode != 0:
        stderr = result.stderr.decode('utf-8', errors='replace') if result.stderr else ''
        if stderr:
            print(f"[sdp] xz error: {stderr.strip()}")
        if temp_path.exists():
            temp_path.unlink()
        raise subprocess.CalledProcessError(
            result.returncode, cmd, stderr=stderr
        )

    if not temp_path.exists():
        raise RuntimeError(f"Decompression produced no output: {temp_path}")

    temp_path.rename(output_path)

    file_size = output_path.stat().st_size
    print(f"[sdp] Decompressed: {output_path.name} ({file_size / (1024**3):.2f} GB)")

    return str(output_path)


# ============================================================================
# Tar+Gzip (.tar.gz, .tgz)
# ============================================================================

def decompress_tar_gz(input_path: str, output_dir: str) -> str:
    """Extract a .tar.gz / .tgz archive using tar CLI.

    Extracts all files into output_dir. Returns the output directory path
    since tar archives can contain multiple files.

    Args:
        input_path: Path to the tar.gz archive
        output_dir: Directory to extract into

    Returns:
        Path to the output directory
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[sdp] Extracting {input_path.name}")

    cmd = ["tar", "-xzf", str(input_path), "-C", str(output_dir)]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        if result.stderr:
            print(f"[sdp] tar error: {result.stderr.strip()}")
        raise subprocess.CalledProcessError(
            result.returncode, cmd, output=result.stdout, stderr=result.stderr
        )

    print(f"[sdp] Extracted: {input_path.name} -> {output_dir}")

    return str(output_dir)
