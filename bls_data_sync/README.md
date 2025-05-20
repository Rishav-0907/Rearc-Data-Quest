# BLS Data Sync Project

This project implements an automated data pipeline to fetch and sync data from the Bureau of Labor Statistics (BLS) to an AWS S3 bucket.

## Setup Instructions

1. Install AWS CLI and configure credentials
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a .env file with your AWS credentials and BLS API configuration

## Project Structure
```
bls_data_sync/
├── src/
│   ├── __init__.py
│   └── bls_sync.py
├── config/
├── logs/
├── requirements.txt
└── README.md
```

## Features
- Automated data fetching from BLS
- S3 storage integration
- Incremental sync mechanism
- Error handling and logging
- Compliance with BLS data access policies

## Usage
```bash
python src/bls_sync.py
```
