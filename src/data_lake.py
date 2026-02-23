#!/usr/bin/env python3
"""
Data lake management system using local filesystem as a simple lakehouse.
Supports partitioned storage of parquet, CSV, and JSON files.
"""

import os
import json
import csv
import sys
from pathlib import Path
from datetime import datetime, timedelta
from shutil import copy2, rmtree
from typing import List, Dict, Any, Callable, Optional


class DataLake:
    """Local filesystem-based data lake management system."""
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize DataLake.
        
        Args:
            base_path: Root path for data lake. Defaults to ~/.blackroad/datalake/
        """
        if base_path is None:
            base_path = os.path.expanduser("~/.blackroad/datalake")
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def ingest(self, source_path: str, dataset_name: str, partition_key: Optional[str] = None) -> str:
        """
        Ingest a file into the data lake with optional partitioning.
        
        Args:
            source_path: Path to source file
            dataset_name: Name of the dataset
            partition_key: Optional partition key (YYYY/MM/DD format or custom)
        
        Returns:
            Destination path where file was ingested
        """
        source_file = Path(source_path)
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        # Determine partition directory
        if partition_key is None:
            now = datetime.now()
            partition_key = f"{now.year:04d}/{now.month:02d}/{now.day:02d}"
        
        dataset_dir = self.base_path / dataset_name / partition_key
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        dest_path = dataset_dir / source_file.name
        copy2(source_file, dest_path)
        
        return str(dest_path)
    
    def query(self, dataset_name: str, filter_fn: Optional[Callable[[Dict], bool]] = None) -> List[Dict[str, Any]]:
        """
        Query all records in a dataset.
        
        Args:
            dataset_name: Name of the dataset
            filter_fn: Optional filter function to apply to records
        
        Returns:
            List of records (dicts)
        """
        dataset_dir = self.base_path / dataset_name
        if not dataset_dir.exists():
            return []
        
        records = []
        
        for file_path in sorted(dataset_dir.rglob("*")):
            if not file_path.is_file():
                continue
            
            if file_path.suffix == ".json":
                records.extend(self._read_json(file_path, filter_fn))
            elif file_path.suffix == ".jsonl":
                records.extend(self._read_jsonl(file_path, filter_fn))
            elif file_path.suffix == ".csv":
                records.extend(self._read_csv(file_path, filter_fn))
        
        return records
    
    def _read_json(self, path: Path, filter_fn: Optional[Callable] = None) -> List[Dict]:
        """Read JSON file."""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    records = data
                else:
                    records = [data]
                
                if filter_fn:
                    records = [r for r in records if filter_fn(r)]
                return records
        except Exception as e:
            print(f"Error reading {path}: {e}", file=sys.stderr)
            return []
    
    def _read_jsonl(self, path: Path, filter_fn: Optional[Callable] = None) -> List[Dict]:
        """Read JSONL (newline-delimited JSON) file."""
        records = []
        try:
            with open(path, 'r') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        if filter_fn is None or filter_fn(record):
                            records.append(record)
        except Exception as e:
            print(f"Error reading {path}: {e}", file=sys.stderr)
        
        return records
    
    def _read_csv(self, path: Path, filter_fn: Optional[Callable] = None) -> List[Dict]:
        """Read CSV file."""
        records = []
        try:
            with open(path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if filter_fn is None or filter_fn(row):
                        records.append(row)
        except Exception as e:
            print(f"Error reading {path}: {e}", file=sys.stderr)
        
        return records
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all datasets with metadata.
        
        Returns:
            List of dicts with: name, size_mb, file_count, last_updated
        """
        datasets = []
        
        if not self.base_path.exists():
            return datasets
        
        for dataset_dir in sorted(self.base_path.iterdir()):
            if not dataset_dir.is_dir():
                continue
            
            dataset_name = dataset_dir.name
            size_bytes = 0
            file_count = 0
            last_modified = None
            
            for file_path in dataset_dir.rglob("*"):
                if file_path.is_file():
                    size_bytes += file_path.stat().st_size
                    file_count += 1
                    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if last_modified is None or mtime > last_modified:
                        last_modified = mtime
            
            datasets.append({
                "name": dataset_name,
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "file_count": file_count,
                "last_updated": last_modified.isoformat() if last_modified else None
            })
        
        return datasets
    
    def get_schema(self, dataset_name: str) -> Dict[str, str]:
        """
        Infer schema from first file in dataset.
        
        Args:
            dataset_name: Name of the dataset
        
        Returns:
            Dict mapping field names to inferred types
        """
        dataset_dir = self.base_path / dataset_name
        if not dataset_dir.exists():
            return {}
        
        # Find first readable file
        for file_path in sorted(dataset_dir.rglob("*")):
            if not file_path.is_file():
                continue
            
            if file_path.suffix == ".json":
                return self._infer_schema_json(file_path)
            elif file_path.suffix == ".csv":
                return self._infer_schema_csv(file_path)
            elif file_path.suffix == ".jsonl":
                return self._infer_schema_jsonl(file_path)
        
        return {}
    
    def _infer_schema_json(self, path: Path) -> Dict[str, str]:
        """Infer schema from JSON file."""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    sample = data[0]
                else:
                    sample = data
                
                return {k: type(v).__name__ for k, v in sample.items()}
        except Exception as e:
            print(f"Error inferring schema: {e}", file=sys.stderr)
            return {}
    
    def _infer_schema_jsonl(self, path: Path) -> Dict[str, str]:
        """Infer schema from JSONL file."""
        try:
            with open(path, 'r') as f:
                first_line = f.readline().strip()
                if first_line:
                    sample = json.loads(first_line)
                    return {k: type(v).__name__ for k, v in sample.items()}
        except Exception as e:
            print(f"Error inferring schema: {e}", file=sys.stderr)
        
        return {}
    
    def _infer_schema_csv(self, path: Path) -> Dict[str, str]:
        """Infer schema from CSV file (all fields treated as strings)."""
        try:
            with open(path, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                return {h: "str" for h in headers}
        except Exception as e:
            print(f"Error inferring schema: {e}", file=sys.stderr)
        
        return {}
    
    def vacuum(self, dataset_name: str, keep_days: int = 30) -> int:
        """
        Remove old partitions outside the retention period.
        
        Args:
            dataset_name: Name of the dataset
            keep_days: Number of days to keep (default 30)
        
        Returns:
            Number of partitions removed
        """
        dataset_dir = self.base_path / dataset_name
        if not dataset_dir.exists():
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        removed_count = 0
        
        for partition_dir in dataset_dir.iterdir():
            if not partition_dir.is_dir():
                continue
            
            try:
                # Try to parse YYYY/MM/DD format
                parts = str(partition_dir.relative_to(dataset_dir)).split(os.sep)
                if len(parts) >= 3:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    partition_date = datetime(year, month, day)
                    
                    if partition_date < cutoff_date:
                        rmtree(partition_dir)
                        removed_count += 1
            except (ValueError, IndexError):
                pass
        
        return removed_count
    
    def stats(self) -> Dict[str, Any]:
        """
        Get data lake statistics.
        
        Returns:
            Dict with: total_size_mb, dataset_count, estimated_record_count
        """
        total_size = 0
        dataset_count = 0
        record_count_estimate = 0
        
        if not self.base_path.exists():
            return {
                "total_size_mb": 0,
                "dataset_count": 0,
                "estimated_record_count": 0
            }
        
        for dataset_dir in self.base_path.iterdir():
            if not dataset_dir.is_dir():
                continue
            
            dataset_count += 1
            
            for file_path in dataset_dir.rglob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
                    
                    # Rough estimate: count lines in files
                    if file_path.suffix in [".csv", ".jsonl"]:
                        try:
                            with open(file_path, 'r') as f:
                                record_count_estimate += sum(1 for _ in f)
                        except:
                            pass
        
        return {
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "dataset_count": dataset_count,
            "estimated_record_count": record_count_estimate
        }


def main():
    """CLI interface for DataLake."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Lake Management System")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest a file into the data lake")
    ingest_parser.add_argument("source", help="Source file path")
    ingest_parser.add_argument("dataset", help="Dataset name")
    ingest_parser.add_argument("--partition", help="Partition key (YYYY/MM/DD format)")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Query a dataset")
    query_parser.add_argument("dataset", help="Dataset name")
    
    # List command
    subparsers.add_parser("list", help="List all datasets")
    
    # Stats command
    subparsers.add_parser("stats", help="Show data lake statistics")
    
    # Schema command
    schema_parser = subparsers.add_parser("schema", help="Get dataset schema")
    schema_parser.add_argument("dataset", help="Dataset name")
    
    # Vacuum command
    vacuum_parser = subparsers.add_parser("vacuum", help="Clean old partitions")
    vacuum_parser.add_argument("dataset", help="Dataset name")
    vacuum_parser.add_argument("--keep-days", type=int, default=30, help="Days to keep")
    
    args = parser.parse_args()
    lake = DataLake()
    
    if args.command == "ingest":
        dest = lake.ingest(args.source, args.dataset, args.partition)
        print(f"Ingested: {dest}")
    elif args.command == "query":
        records = lake.query(args.dataset)
        print(json.dumps(records, indent=2))
    elif args.command == "list":
        datasets = lake.list_datasets()
        print(json.dumps(datasets, indent=2))
    elif args.command == "stats":
        stats = lake.stats()
        print(json.dumps(stats, indent=2))
    elif args.command == "schema":
        schema = lake.get_schema(args.dataset)
        print(json.dumps(schema, indent=2))
    elif args.command == "vacuum":
        removed = lake.vacuum(args.dataset, args.keep_days)
        print(f"Removed {removed} partitions")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
