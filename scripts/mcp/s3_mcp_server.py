#!/usr/bin/env python3
"""
MCP server for S3 that never writes to disk.

All tools return data (bytes as base64) and/or metadata so the *agent*
decides where and how to persist files (e.g., inside its workspace via a
controlled write_file tool).

Environment used:
- AWS_REGION / AWS_PROFILE / standard AWS_* creds
- MAX_INLINE_BYTES (optional): hard cap for s3_get_object_bytes; default 33554432 (32 MiB)
"""

import base64
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("s3")

# --------------- helpers ---------------


def _s3():
    """Create a low-friction S3 client honoring env config."""
    return boto3.client("s3", region_name=os.getenv("AWS_REGION"))


def _to_iso8601(dt) -> Optional[str]:
    if not dt:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    try:
        return str(dt)
    except Exception:
        return None


def _max_inline_bytes() -> int:
    try:
        return int(
            os.getenv("MAX_INLINE_BYTES", str(32 * 1024 * 1024))
        )  # 32 MiB
    except Exception:
        return 32 * 1024 * 1024


# --------------- tools ---------------


@mcp.tool(name="s3_whoami")
def s3_whoami() -> dict:
    """Return the STS caller identity & visible env bits (no secrets)."""
    import os

    import boto3

    env_view = {
        "AWS_REGION": os.getenv("AWS_REGION"),
        "AWS_PROFILE": os.getenv("AWS_PROFILE"),
        "HAS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID" in os.environ,
        "HAS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY" in os.environ,
        "HAS_SESSION_TOKEN": "AWS_SESSION_TOKEN" in os.environ,
        "HOME": os.getenv("HOME"),
    }
    sts = boto3.client("sts", region_name=os.getenv("AWS_REGION"))
    ident = sts.get_caller_identity()
    return {"env_view": env_view, "caller_identity": ident}


@mcp.tool(name="s3_list_objects")
def s3_list_objects(
    bucket: str, prefix: str = "", max_keys: int = 1000
) -> Dict[str, Any]:
    """
    List up to max_keys objects under bucket/prefix.
    Returns keys and sizes (no bodies).
    """

    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")

    out: List[Dict[str, Any]] = []
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            out.append({
                "key": obj["Key"],
                "size": obj.get("Size"),
                "last_modified": _to_iso8601(obj.get("LastModified")),
                "storage_class": obj.get("StorageClass"),
            })
            total += 1
            if total >= max_keys:
                return {
                    "bucket": bucket,
                    "prefix": prefix,
                    "count": total,
                    "objects": out,
                }
    return {"bucket": bucket, "prefix": prefix, "count": total, "objects": out}


@mcp.tool(name="s3_head_object")
def s3_head_object(bucket: str, key: str) -> Dict[str, Any]:
    """
    Return object metadata (size, ETag, content-type, etc.) without body.
    """
    s3 = _s3()
    resp = s3.head_object(Bucket=bucket, Key=key)

    return {
        "bucket": bucket,
        "key": key,
        "size": resp.get("ContentLength"),
        "etag": resp.get("ETag"),
        "content_type": resp.get("ContentType"),
        "content_encoding": resp.get("ContentEncoding"),
        "last_modified": _to_iso8601(resp.get("LastModified")),
        "metadata": resp.get("Metadata", {}),
    }


@mcp.tool(name="s3_get_object_bytes")
def s3_get_object_bytes(
    bucket: str,
    key: str,
    allow_large: bool = False,
    cap_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Return the entire object as base64-encoded bytes.

    Safety:
    - By default, enforces a max payload size (MAX_INLINE_BYTES env, default 32 MiB).
    - Set allow_large=True to bypass the cap (use with caution).
    - You can also pass cap_bytes to override the env cap for this call.
    """
    s3 = _s3()

    # Determine the allowed max
    limit = cap_bytes if cap_bytes is not None else _max_inline_bytes()

    # Preflight head to learn size and metadata
    h = s3.head_object(Bucket=bucket, Key=key)
    size = int(h.get("ContentLength", 0))

    if not allow_large and size > limit:
        return {
            "bucket": bucket,
            "key": key,
            "size": size,
            "status": "too_large",
            "limit": limit,
            "note": "Object exceeds inline bytes cap; use s3_read_range_bytes to stream in chunks "
            "or set allow_large=True if you know what you're doing.",
        }

    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    return {
        "bucket": bucket,
        "key": key,
        "size": size,
        "bytes_b64": base64.b64encode(data).decode("ascii"),
        "encoding": "base64",
        "content_type": h.get("ContentType"),
        "etag": h.get("ETag"),
        "last_modified": _to_iso8601(h.get("LastModified")),
        "status": "ok",
    }


@mcp.tool(name="s3_read_range_bytes")
def s3_read_range_bytes(
    bucket: str, key: str, start: int = 0, length: int = 1_048_576
) -> Dict[str, Any]:
    """
    Return a byte range as base64 (use to stream large objects).
    - start: starting offset (0-based)
    - length: number of bytes to read
    Returns: bytes_b64, size (bytes returned), eof flag.
    """
    if length <= 0:
        return {
            "bucket": bucket,
            "key": key,
            "start": start,
            "size": 0,
            "bytes_b64": "",
            "encoding": "base64",
            "eof": True,
        }

    end = start + length - 1
    s3 = _s3()
    obj = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    data = obj["Body"].read()

    # Check EOF by comparing returned size vs requested length
    eof = len(data) < length

    return {
        "bucket": bucket,
        "key": key,
        "start": start,
        "size": len(data),
        "bytes_b64": base64.b64encode(data).decode("ascii"),
        "encoding": "base64",
        "eof": eof,
        # Optional: expose content-range if present
        "content_range": obj.get("ContentRange"),
        "status": "ok",
    }


@mcp.tool(name="s3_read_text_preview")
def s3_read_text_preview(
    bucket: str, key: str, max_bytes: int = 65536, encoding: str = "utf-8"
) -> Dict[str, Any]:
    """
    Convenience: read up to max_bytes and return decoded text (for CSV headers/previews).
    """
    if max_bytes <= 0:
        return {
            "bucket": bucket,
            "key": key,
            "bytes_returned": 0,
            "preview": "",
        }

    s3 = _s3()
    obj = s3.get_object(
        Bucket=bucket, Key=key, Range=f"bytes=0-{max_bytes - 1}"
    )
    body = obj["Body"].read()
    try:
        text = body.decode(encoding, errors="replace")
    except Exception:
        text = body.decode("utf-8", errors="replace")

    return {
        "bucket": bucket,
        "key": key,
        "bytes_returned": len(body),
        "encoding_used": encoding,
        "preview": text,
    }


if __name__ == "__main__":
    mcp.run()
