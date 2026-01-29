"""Utility functions to read files from Azure Blob Storage.

Supports connection via connection string or DefaultAzureCredential.

Functions:
- list_blobs(container_name, prefix=None)
- download_blob_to_bytes(container_name, blob_name)
- download_blob_to_text(container_name, blob_name, encoding='utf-8', fallback_encodings=None)
- guess_content_type(data: bytes) -> str

Usage:
    from blob_reader import download_blob_to_text
    text = download_blob_to_text('my-container', 'path/to/file.txt')
"""
from typing import List, Optional, Tuple, Union
import os
import codecs
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover - azure SDK might not be installed in dev env
    BlobServiceClient = None  # type: ignore

from azure.identity import DefaultAzureCredential


def _create_blob_service_client() -> BlobServiceClient:
    """Create BlobServiceClient.

    Authentication preference and environment variables checked:
    1. AZURE_STORAGE_ACCOUNT_URL with DefaultAzureCredential (preferred when key auth is disabled)
    2. AZURE_STORAGE_CONNECTION_STRING (fallback)

    Raises helpful errors when configuration is invalid or when key-based auth is not allowed.
    """
    if BlobServiceClient is None:
        raise ImportError("azure-storage-blob is required. Install with: pip install azure-storage-blob")

    # Prefer AAD authentication (account_url + DefaultAzureCredential)
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    if account_url:
        try:
            credential = DefaultAzureCredential()
            return BlobServiceClient(account_url=account_url, credential=credential)
        except Exception as e:
            # If key-based auth is disabled, attempting from_connection_string will fail too; surface the message
            raise RuntimeError(
                "Failed to create BlobServiceClient with DefaultAzureCredential. Ensure your environment is authenticated (Azure CLI, Managed Identity) and AZURE_STORAGE_ACCOUNT_URL is correct. Original error: %s" % e
            )

    # Fallback to connection string (only if allowed by the storage account)
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        try:
            return BlobServiceClient.from_connection_string(conn_str)
        except Exception as e:
            # Detect key-based auth disabled
            msg = str(e)
            if "KeyBasedAuthenticationNotPermitted" in msg or "Key based authentication is not permitted" in msg:
                raise RuntimeError(
                    "Key-based authentication (connection string / account key) is not permitted for this storage account.\n"
                    "Use Azure AD authentication instead: set AZURE_STORAGE_ACCOUNT_URL and authenticate with DefaultAzureCredential (Azure CLI, Managed Identity).\n"
                    "See: https://learn.microsoft.com/azure/storage/common/storage-auth-aad"
                )
            raise

    raise ValueError(
        "No Azure Storage connection configuration found. Set AZURE_STORAGE_ACCOUNT_URL (preferred) or AZURE_STORAGE_CONNECTION_STRING.\n"
        "If key-based auth is disabled on the storage account, use AZURE_STORAGE_ACCOUNT_URL and authenticate via Azure AD (Azure CLI, Managed Identity)."
    )


def list_blobs(container_name: str, prefix: Optional[str] = None) -> List[str]:
    """Return a list of blob names in the given container (optionally filtered by prefix)."""
    service = _create_blob_service_client()
    container_client = service.get_container_client(container_name)
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]


def download_blob_to_bytes(container_name: str, blob_name: str) -> bytes:
    """Download the specified blob and return its contents as bytes."""
    service = _create_blob_service_client()
    blob_client = service.get_blob_client(container=container_name, blob=blob_name)
    downloader = blob_client.download_blob()
    return downloader.readall()


def guess_content_type(data: bytes, sample_size: int = 1024) -> str:
    """Guess if content is text or binary by checking for common binary signatures and NUL bytes.
    
    Returns 'text' or 'binary'.
    """
    # Check for common binary file signatures
    binary_sigs = [
        b'%PDF-',  # PDF
        b'PK\x03\x04',  # ZIP
        b'\x89PNG',  # PNG
        b'\xFF\xD8\xFF',  # JPEG
        bytes([0x1f, 0x8b]),  # gzip
    ]
    
    sample = data[:sample_size]
    for sig in binary_sigs:
        if sample.startswith(sig):
            return 'binary'
    
    # Count NUL and control bytes (except common whitespace)
    control_chars = sum(1 for b in sample if b < 32 and b not in {9, 10, 13})  # tab, LF, CR ok
    if control_chars > len(sample) * 0.3:  # >30% control chars suggests binary
        return 'binary'
    
    return 'text'


def safe_decode(data: bytes, primary_encoding: str = 'utf-8',
                fallback_encodings: Optional[List[str]] = None) -> Tuple[str, str]:
    """Attempt to decode bytes as text, trying multiple encodings.
    
    Returns (decoded_text, encoding_used).
    Raises UnicodeError if all encodings fail.
    """
    if not fallback_encodings:
        fallback_encodings = ['latin1', 'cp1252', 'ascii']
    
    # Detect if this is likely binary content
    content_type = guess_content_type(data)
    if content_type == 'binary':
        raise ValueError(
            f"Content appears to be binary data, not text. First few bytes: {data[:20].hex()}. "
            "Use download_blob_to_bytes() for binary content."
        )
    
    # Try primary encoding first
    try:
        return data.decode(primary_encoding), primary_encoding
    except UnicodeError:
        pass  # Try fallbacks
    
    # Try each fallback encoding
    for enc in fallback_encodings:
        try:
            return data.decode(enc), enc
        except UnicodeError:
            continue
    
    # If all encodings failed, raise with helpful message
    raise UnicodeError(
        f"Failed to decode content as text. Tried encodings: {[primary_encoding] + fallback_encodings}. "
        f"First 100 bytes: {data[:100].hex()}. "
        "Specify a different encoding or use download_blob_to_bytes() for binary content."
    )


def download_blob_to_text(
    container_name: str,
    blob_name: str,
    encoding: str = "utf-8",
    fallback_encodings: Optional[List[str]] = None,
) -> Union[str, Tuple[str, str]]:
    """Download blob and decode it to text, with smart encoding detection and fallbacks.
    
    Args:
        container_name: Azure Storage container name
        blob_name: Name/path of the blob to download
        encoding: Primary encoding to try (default: utf-8)
        fallback_encodings: List of fallback encodings to try if primary fails
                          (default: ['latin1', 'cp1252', 'ascii'])
    
    Returns:
        str: The decoded text content
        
    Raises:
        ValueError: If content appears to be binary
        UnicodeError: If all encoding attempts fail
    """
    data = download_blob_to_bytes(container_name, blob_name)
    
    # Use extension to inform encoding choice for common types
    ext = Path(blob_name).suffix.lower()
    if ext in {'.csv', '.tsv'} and encoding == 'utf-8':
        # CSVs often use different encodings; try common ones
        fallback_encodings = ['latin1', 'cp1252', 'utf-16', 'ascii']
    
    decoded, used_encoding = safe_decode(data, encoding, fallback_encodings)
    if used_encoding != encoding:
        # Alert caller that we used a different encoding
        return decoded, used_encoding
    return decoded


__all__ = [
    "list_blobs",
    "download_blob_to_bytes",
    "download_blob_to_text",
    "guess_content_type",
]
