import hashlib


def url_hash(url: str) -> str:
    """Return SHA-256 hex digest of a URL for deduplication."""
    return hashlib.sha256(url.encode()).hexdigest()
