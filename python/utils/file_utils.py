
import hashlib
from pathlib import Path


def calculate_file_checksum(file_path):
    """
    Calculate the SHA-256 checksum of a file.

    Reads the file in chunks to avoid loading
    the entire file into memory.
    """

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b""
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


def get_file_size(file_path):
    """Return file size in bytes."""

    return Path(file_path).stat().st_size
