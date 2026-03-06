"""MongoDB <-> S3 backup and restore for newsscrapper.

Usage:
    python db_backup.py backup              # Backup all collections to S3
    python db_backup.py backup --local      # Backup to local db_backups/ only
    python db_backup.py restore             # Download latest backup from S3 and restore
    python db_backup.py restore --local     # Restore from local db_backups/
    python db_backup.py list                # List available S3 backups
"""

import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from bson import ObjectId, json_util
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "newsscrapper")
S3_BUCKET = os.getenv("S3_BACKUP_BUCKET", "zenflow-db-backups")
S3_PREFIX = "newsscrapper/"
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
LOCAL_BACKUP_DIR = Path("db_backups")

COLLECTIONS = ["articles", "pib_releases", "pib_analysis", "pib_company_links"]


def get_mongo():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def get_s3():
    return boto3.client("s3", region_name=AWS_REGION)


def _serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict."""
    return json.loads(json_util.dumps(doc))


def _deserialize_doc(data):
    """Convert JSON dict back to MongoDB-compatible dict."""
    return json_util.loads(json.dumps(data))


# ── BACKUP ──────────────────────────────────────────────────────────


def backup(local_only=False):
    """Export all collections to gzipped JSON files and upload to S3."""
    db = get_mongo()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_dir = LOCAL_BACKUP_DIR / timestamp
    backup_dir.mkdir(parents=True, exist_ok=True)

    stats = {}
    for coll_name in COLLECTIONS:
        coll = db[coll_name]
        count = coll.count_documents({})
        if count == 0:
            print(f"  {coll_name}: empty, skipping")
            continue

        file_path = backup_dir / f"{coll_name}.json.gz"
        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            docs = list(coll.find({}))
            serialized = [_serialize_doc(d) for d in docs]
            json.dump(serialized, f)

        size_mb = file_path.stat().st_size / (1024 * 1024)
        stats[coll_name] = {"count": count, "size_mb": round(size_mb, 2)}
        print(f"  {coll_name}: {count} docs -> {size_mb:.1f} MB")

    # Write metadata
    meta = {
        "timestamp": timestamp,
        "db": MONGO_DB,
        "collections": stats,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = backup_dir / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    if local_only:
        print(f"\nLocal backup saved to: {backup_dir}")
        return

    # Upload to S3
    s3 = get_s3()
    s3_key_base = f"{S3_PREFIX}{timestamp}"
    for file in backup_dir.iterdir():
        s3_key = f"{s3_key_base}/{file.name}"
        print(f"  Uploading {file.name} -> s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(str(file), S3_BUCKET, s3_key)

    print(f"\nBackup complete: s3://{S3_BUCKET}/{s3_key_base}/")
    return timestamp


# ── RESTORE ─────────────────────────────────────────────────────────


def restore(local_only=False, backup_id=None):
    """Download backup from S3 and restore to MongoDB."""
    if local_only:
        # Find latest local backup
        if backup_id:
            backup_dir = LOCAL_BACKUP_DIR / backup_id
        else:
            dirs = sorted(LOCAL_BACKUP_DIR.iterdir()) if LOCAL_BACKUP_DIR.exists() else []
            if not dirs:
                print("No local backups found")
                return
            backup_dir = dirs[-1]
        print(f"Restoring from local: {backup_dir}")
    else:
        # Download from S3
        s3 = get_s3()
        if not backup_id:
            # Find latest backup
            backups = list_backups_s3()
            if not backups:
                print("No S3 backups found")
                return
            backup_id = backups[-1]

        backup_dir = LOCAL_BACKUP_DIR / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)

        s3_prefix = f"{S3_PREFIX}{backup_id}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            local_path = backup_dir / filename
            if not local_path.exists():
                print(f"  Downloading {filename}")
                s3.download_file(S3_BUCKET, key, str(local_path))

        print(f"Downloaded backup: {backup_id}")

    # Restore each collection
    db = get_mongo()
    for gz_file in backup_dir.glob("*.json.gz"):
        coll_name = gz_file.stem.replace(".json", "")
        if coll_name not in COLLECTIONS:
            continue

        with gzip.open(gz_file, "rt", encoding="utf-8") as f:
            docs = json.load(f)

        docs = [_deserialize_doc(d) for d in docs]
        if not docs:
            continue

        coll = db[coll_name]
        existing = coll.count_documents({})

        # Upsert approach: skip docs that already exist (by _id)
        inserted = 0
        skipped = 0
        for doc in docs:
            doc_id = doc.get("_id")
            if doc_id and coll.find_one({"_id": doc_id}):
                skipped += 1
            else:
                try:
                    coll.insert_one(doc)
                    inserted += 1
                except Exception:
                    skipped += 1

        print(f"  {coll_name}: {inserted} inserted, {skipped} skipped (existing: {existing})")

    print("\nRestore complete")


# ── LIST ────────────────────────────────────────────────────────────


def list_backups_s3():
    """List available S3 backups."""
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX, Delimiter="/")
    prefixes = [p["Prefix"].rstrip("/").split("/")[-1]
                for p in resp.get("CommonPrefixes", [])]
    return sorted(prefixes)


def list_backups():
    """List S3 and local backups."""
    print("S3 backups:")
    try:
        backups = list_backups_s3()
        for b in backups:
            print(f"  {b}")
        if not backups:
            print("  (none)")
    except Exception as e:
        print(f"  Error: {e}")

    print("\nLocal backups:")
    if LOCAL_BACKUP_DIR.exists():
        for d in sorted(LOCAL_BACKUP_DIR.iterdir()):
            if d.is_dir():
                meta_file = d / "metadata.json"
                if meta_file.exists():
                    with open(meta_file) as f:
                        meta = json.load(f)
                    colls = meta.get("collections", {})
                    total = sum(c["count"] for c in colls.values())
                    print(f"  {d.name} — {total} docs, {len(colls)} collections")
                else:
                    print(f"  {d.name}")
    else:
        print("  (none)")


# ── CLI ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="MongoDB backup/restore to S3")
    parser.add_argument("action", choices=["backup", "restore", "list"])
    parser.add_argument("--local", action="store_true", help="Local only (no S3)")
    parser.add_argument("--id", default=None, help="Specific backup ID (timestamp)")
    args = parser.parse_args()

    if args.action == "backup":
        print(f"Backing up {MONGO_DB}...")
        backup(local_only=args.local)
    elif args.action == "restore":
        restore(local_only=args.local, backup_id=args.id)
    elif args.action == "list":
        list_backups()


if __name__ == "__main__":
    main()
