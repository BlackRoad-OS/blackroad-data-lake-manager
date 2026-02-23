# BlackRoad Data Lake Manager

A lightweight, filesystem-based data lake management system that serves as a simple lakehouse. Manages partitioned data storage with support for multiple file formats.

## Features

- **Partitioned Storage**: Organize data with YYYY/MM/DD partitioning
- **Multi-Format Support**: JSON, JSONL, and CSV file support
- **Schema Inference**: Automatically infer schemas from data files
- **Data Retention**: Vacuum (cleanup) old partitions based on retention policy
- **Query Interface**: Filter and retrieve records with optional custom filters
- **Statistics**: Track total size, dataset counts, and record estimates
- **CLI**: Command-line interface for all operations

## Installation

No external dependencies required - uses only Python standard library.

```bash
python -m pip install -r requirements.txt  # Empty by design
```

## Usage

### Python API

```python
from src.data_lake import DataLake

lake = DataLake()

# Ingest data
dest = lake.ingest("data.csv", "my-dataset")

# Query data
records = lake.query("my-dataset")

# List datasets
datasets = lake.list_datasets()

# Get schema
schema = lake.get_schema("my-dataset")

# Get statistics
stats = lake.stats()

# Clean old partitions
removed = lake.vacuum("my-dataset", keep_days=30)
```

### CLI

```bash
# Ingest a file
python src/data_lake.py ingest ./data.csv my-dataset

# Ingest with custom partition
python src/data_lake.py ingest ./data.csv my-dataset --partition 2024/02/22

# Query a dataset
python src/data_lake.py query my-dataset

# List all datasets
python src/data_lake.py list

# Get dataset schema
python src/data_lake.py schema my-dataset

# View statistics
python src/data_lake.py stats

# Vacuum old partitions
python src/data_lake.py vacuum my-dataset --keep-days 30
```

## Data Lake Location

By default, the data lake is stored in `~/.blackroad/datalake/`

## Partition Format

Data is organized as: `{dataset_name}/{partition_key}/{filename}`

Default partition key format: `YYYY/MM/DD/`

## File Formats

- **JSON**: Array or single object
- **JSONL**: Newline-delimited JSON objects
- **CSV**: Comma-separated values with headers

## Testing

```bash
python -m pytest src/ -v
```
