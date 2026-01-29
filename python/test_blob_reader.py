"""Example usage of blob_reader with error handling.

Set one of the following environment variables before running:
- AZURE_STORAGE_ACCOUNT_URL (recommended, use with Azure CLI auth or Managed Identity)
OR
- AZURE_STORAGE_CONNECTION_STRING (if key-based auth is allowed)

Optional env vars:
- TEST_BLOB_CONTAINER: container name (default: "sample-container")
- TEST_BLOB_PREFIX: path prefix to list (default: None, list all)
- TEST_BLOB_ENCODING: primary encoding to try (default: "utf-8")

Example setup (using Azure CLI auth):
    az login  # authenticate
    export AZURE_STORAGE_ACCOUNT_URL="https://myaccount.blob.core.windows.net/"
    export TEST_BLOB_CONTAINER="my-container"
    python3 python/test_blob_reader.py

Or with connection string (if allowed):
    export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
    python3 python/test_blob_reader.py
"""
import os
import sys
from pathlib import Path
from typing import Optional, List, Tuple

from blob_reader import (
    list_blobs,
    download_blob_to_text,
    download_blob_to_bytes,
    guess_content_type
)


def format_size(size_bytes: int) -> str:
    """Format bytes as human readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} GB"


def download_and_show(
    container: str,
    blob_name: str,
    encoding: str = 'utf-8',
    fallback_encodings: Optional[List[str]] = None,
    max_preview: int = 1000
) -> None:
    """Download a blob and show its contents, handling binary vs text appropriately."""
    print(f"\nDownloading blob: {blob_name}")
    
    try:
        # Try text first
        result = download_blob_to_text(
            container,
            blob_name,
            encoding=encoding,
            fallback_encodings=fallback_encodings
        )
        
        # Check if we got back (text, encoding) or just text
        if isinstance(result, tuple):
            text, used_encoding = result
            if used_encoding != encoding:
                print(f"Note: decoded using {used_encoding} (not {encoding})")
        else:
            text = result
        
        print("Preview (text):")
        print("-" * 40)
        print(text[:max_preview])
        if len(text) > max_preview:
            print(f"... ({len(text)} chars total)")
        print("-" * 40)
        
    except ValueError as e:
        if "appears to be binary" in str(e):
            # Handle binary content
            print("Detected binary content, downloading as bytes...")
            data = download_blob_to_bytes(container, blob_name)
            content_type = guess_content_type(data)
            print(f"Content type: {content_type}")
            print(f"Size: {format_size(len(data))}")
            print("First 32 bytes (hex):", data[:32].hex())
        else:
            raise
            
    except UnicodeError as e:
        print(f"Error: Failed to decode as text: {e}")
        should_try_binary = input("Try downloading as binary instead? [y/N] ").lower()
        if should_try_binary.startswith('y'):
            data = download_blob_to_bytes(container, blob_name)
            print(f"Downloaded {format_size(len(data))} of binary data")
            print("First 32 bytes (hex):", data[:32].hex())


def main():
    # Get settings from environment
    container = os.getenv("TEST_BLOB_CONTAINER") or "sample-container"
    prefix = os.getenv("TEST_BLOB_PREFIX")
    encoding = os.getenv("TEST_BLOB_ENCODING", "utf-8")
    
    # List blobs (with error handling)
    print(f"Listing blobs in container={container} prefix={prefix or '(none)'}")
    try:
        blobs = list_blobs(container, prefix=prefix)
    except Exception as e:
        print("Error listing blobs:", e, file=sys.stderr)
        if "KeyBasedAuthenticationNotPermitted" in str(e):
            print("\nTip: This storage account requires Azure AD authentication.", file=sys.stderr)
            print("Run 'az login' first, then set AZURE_STORAGE_ACCOUNT_URL.", file=sys.stderr)
        elif "AuthenticationFailed" in str(e):
            print("\nTip: Azure AD authentication failed.", file=sys.stderr)
            print("1. Ensure you're logged in: az login", file=sys.stderr)
            print("2. Check AZURE_STORAGE_ACCOUNT_URL is correct", file=sys.stderr)
            print("3. Verify you have appropriate RBAC roles (e.g., Storage Blob Data Reader)", file=sys.stderr)
        return 1

    # Show what we found
    print(f"\nFound {len(blobs)} blob(s):")
    for b in sorted(blobs)[:10]:
        print(" -", b)
    if len(blobs) > 10:
        print(f"... and {len(blobs) - 10} more")

    # Download and show first matching blob
    if blobs:
        blob_to_read = blobs[0]
        try:
            download_and_show(
                container,
                blob_to_read,
                encoding=encoding,
                fallback_encodings=['latin1', 'cp1252', 'utf-16']
            )
        except Exception as e:
            print(f"Error downloading/displaying blob: {type(e).__name__}: {e}", file=sys.stderr)
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
