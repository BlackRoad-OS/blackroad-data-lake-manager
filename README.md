<!-- BlackRoad SEO Enhanced -->

# ulackroad data lake manager

> Part of **[BlackRoad OS](https://blackroad.io)** — Sovereign Computing for Everyone

[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-ff1d6c?style=for-the-badge)](https://blackroad.io)
[![BlackRoad OS](https://img.shields.io/badge/Org-BlackRoad-OS-2979ff?style=for-the-badge)](https://github.com/BlackRoad-OS)
[![License](https://img.shields.io/badge/License-Proprietary-f5a623?style=for-the-badge)](LICENSE)

**ulackroad data lake manager** is part of the **BlackRoad OS** ecosystem — a sovereign, distributed operating system built on edge computing, local AI, and mesh networking by **BlackRoad OS, Inc.**

## About BlackRoad OS

BlackRoad OS is a sovereign computing platform that runs AI locally on your own hardware. No cloud dependencies. No API keys. No surveillance. Built by [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc), a Delaware C-Corp founded in 2025.

### Key Features
- **Local AI** — Run LLMs on Raspberry Pi, Hailo-8, and commodity hardware
- **Mesh Networking** — WireGuard VPN, NATS pub/sub, peer-to-peer communication
- **Edge Computing** — 52 TOPS of AI acceleration across a Pi fleet
- **Self-Hosted Everything** — Git, DNS, storage, CI/CD, chat — all sovereign
- **Zero Cloud Dependencies** — Your data stays on your hardware

### The BlackRoad Ecosystem
| Organization | Focus |
|---|---|
| [BlackRoad OS](https://github.com/BlackRoad-OS) | Core platform and applications |
| [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc) | Corporate and enterprise |
| [BlackRoad AI](https://github.com/BlackRoad-AI) | Artificial intelligence and ML |
| [BlackRoad Hardware](https://github.com/BlackRoad-Hardware) | Edge hardware and IoT |
| [BlackRoad Security](https://github.com/BlackRoad-Security) | Cybersecurity and auditing |
| [BlackRoad Quantum](https://github.com/BlackRoad-Quantum) | Quantum computing research |
| [BlackRoad Agents](https://github.com/BlackRoad-Agents) | Autonomous AI agents |
| [BlackRoad Network](https://github.com/BlackRoad-Network) | Mesh and distributed networking |
| [BlackRoad Education](https://github.com/BlackRoad-Education) | Learning and tutoring platforms |
| [BlackRoad Labs](https://github.com/BlackRoad-Labs) | Research and experiments |
| [BlackRoad Cloud](https://github.com/BlackRoad-Cloud) | Self-hosted cloud infrastructure |
| [BlackRoad Forge](https://github.com/BlackRoad-Forge) | Developer tools and utilities |

### Links
- **Website**: [blackroad.io](https://blackroad.io)
- **Documentation**: [docs.blackroad.io](https://docs.blackroad.io)
- **Chat**: [chat.blackroad.io](https://chat.blackroad.io)
- **Search**: [search.blackroad.io](https://search.blackroad.io)

---


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
