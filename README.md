# MCA Insights Engine

A comprehensive data pipeline and analytics platform for tracking and analyzing company master data from the Ministry of Corporate Affairs (MCA). This system automates the detection of company-level changes, enriches data with publicly available information, and provides AI-powered insights through an interactive dashboard.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [API Documentation](#api-documentation)
- [Technical Details](#technical-details)
- [Troubleshooting](#troubleshooting)

## Overview

The MCA Insights Engine consolidates state-wise company master data, detects daily changes in company registrations and details, enriches records with web-sourced information, and provides both a web dashboard and REST API for querying the data. The system includes an AI-powered chatbot for natural language queries and automated summary generation.

### Problem Statement

MCA publishes company master data as separate state-wise CSV files containing information like CIN, company name, incorporation date, capital details, and status. Manually tracking changes across thousands of records is impractical. This project automates change detection, enrichment, and analysis of this critical data.

## Features

### Core Capabilities

- **Data Integration**: Merges and standardizes data from five major states (Maharashtra, Gujarat, Delhi, Tamil Nadu, Karnataka)
- **Change Detection**: Automatically tracks new incorporations, deregistrations, and field-level updates across daily snapshots
- **Web Enrichment**: Augments company data with publicly available information from sources like ZaubaCorp, GST Portal, and MCA APIs
- **AI-Powered Insights**: Generates daily summaries and provides conversational query interface
- **Interactive Dashboard**: Streamlit-based web interface with search, filters, and visualizations
- **REST API**: RESTful endpoints for programmatic access to company data and changes
- **Database Management**: SQLite database for efficient storage and retrieval

### Key Functionalities

1. Automated daily change tracking and logging
2. Natural language query processing for company data
3. Visualization of trends, distributions, and time-series changes
4. Export capabilities for change logs and enriched datasets
5. Comprehensive search with filters by state, status, and incorporation year

## System Architecture

The system follows a modular pipeline architecture:

```
Raw Data (State CSVs) 
    → Data Integration (Standardization & Cleaning)
    → Snapshot Management
    → Change Detection (Compare Snapshots)
    → Web Enrichment (Public Sources)
    → AI Insights (Summary Generation)
    → Database Population
    → Query Interfaces (Dashboard & API)
```

### Component Overview

- **Data Integration Module**: Consolidates state-wise CSVs into a unified master dataset
- **Change Detection Module**: Compares snapshots to identify new, modified, or removed records
- **Web Enrichment Module**: Fetches additional company information from public sources
- **AI Insights Module**: Generates automated summaries and handles natural language queries
- **Database Module**: Manages SQLite database operations
- **Application Layer**: Streamlit dashboard and Flask REST API

## Project Structure

```
mca-insights-engine/
│
├── data/
│   ├── raw/                    # Original state-wise CSV files
│   ├── processed/              # Cleaned and merged datasets
│   ├── snapshots/              # Daily snapshots for change detection
│   ├── change_logs/            # Daily change logs (CSV/JSON)
│   ├── enriched/               # Enriched company data
│   └── summaries/              # AI-generated summaries
│
├── src/
│   ├── __init__.py
│   ├── data_integration.py     # Data merging and cleaning
│   ├── change_detection.py     # Change tracking logic
│   ├── web_enrichment.py       # Web scraping and enrichment
│   ├── ai_insights.py          # AI summary and chat functionality
│   └── database.py             # Database operations
│
├── app/
│   ├── streamlit_app.py        # Main dashboard application
│   └── api.py                  # REST API endpoints
│
├── config/
│   └── config.yaml             # Configuration settings
│
├── requirements.txt            # Python dependencies
├── setup.sh                    # Initial setup script
├── run_pipeline.sh             # Pipeline execution script
├── generate_sample_data.py     # Sample data generator
└── README.md                   # This file
```

## Prerequisites

### System Requirements

- Python 3.8 or higher
- 4GB RAM minimum (8GB recommended)
- 2GB free disk space
- Internet connection (for web enrichment)

### For Mac M1/M2 Users

If you encounter issues with pandas installation on Apple Silicon, use one of these approaches:

**Option 1: Install via Homebrew**
```bash
brew install python@3.11
```

**Option 2: Use Conda**
```bash
conda install pandas numpy
```

**Option 3: Update requirements.txt**
Replace `pandas==2.1.4` with `pandas>=2.0.0` for better M1 compatibility.

## Installation

### Step 1: Clone or Download the Repository

```bash
cd /path/to/your/workspace
# If using git:
git clone <repository-url>
cd mca-insights-engine

# If downloaded as zip:
unzip mca-insights-engine.zip
cd mca-insights-engine
```

### Step 2: Set Up Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Mac/Linux
# venv\Scripts\activate  # On Windows
```

### Step 3: Upgrade pip

```bash
pip install --upgrade pip
```

### Step 4: Install Dependencies

```bash
pip install -r requirements.txt
```

If installation fails on Mac M1/M2, try:
```bash
pip install pandas numpy streamlit plotly requests beautifulsoup4 sqlalchemy python-dotenv pyyaml faker flask flask-restful lxml openpyxl
```

### Step 5: Configure the System

Create or verify the `.env` file:
```bash
cp .env.example .env  # If example exists
# Or create manually:
echo "DATABASE_URL=sqlite:///data/mca_master.db" > .env
```

### Step 6: Run Initial Setup

```bash
bash setup.sh
```

This creates all necessary directories and configuration files.

## Usage

### Option 1: Run Complete Pipeline

Execute all modules in sequence:

```bash
bash run_pipeline.sh
```

This will:
1. Integrate state-wise data
2. Detect changes between snapshots
3. Enrich company records
4. Generate AI summaries
5. Populate the database

### Option 2: Run Modules Individually

**Generate Sample Data** (if needed):
```bash
python generate_sample_data.py
```

**Data Integration**:
```bash
python src/data_integration.py
```

**Change Detection**:
```bash
python src/change_detection.py
```

**Web Enrichment**:
```bash
python src/web_enrichment.py
```

**AI Insights**:
```bash
python src/ai_insights.py
```

**Database Population**:
```bash
python src/database.py
```

### Launch the Dashboard

```bash
streamlit run app/streamlit_app.py
```

Access at: `http://localhost:8501`

### Launch the API Server

```bash
python app/api.py
```

Access at: `http://localhost:5000`

## Data Pipeline

### 1. Data Integration

**Purpose**: Merge state-wise CSV files into a canonical master dataset.

**Process**:
- Loads CSV files from `data/raw/`
- Standardizes column names and data types
- Removes duplicates based on CIN
- Validates data quality
- Outputs to `data/processed/current_master.csv`

**Key Features**:
- Handles missing values
- Validates CIN format (21 characters)
- Assigns data quality scores
- Creates timestamped snapshots

### 2. Change Detection

**Purpose**: Track daily changes in company records.

**Process**:
- Compares consecutive snapshots from `data/snapshots/`
- Identifies three change types:
  - NEW_INCORPORATION: Companies appearing in new snapshot
  - DEREGISTRATION: Companies removed from registry
  - FIELD_UPDATE: Changes in company details (capital, status, etc.)
- Generates structured logs in CSV and JSON formats

**Output Format**:
```csv
CIN,COMPANY_NAME,CHANGE_TYPE,FIELD_CHANGED,OLD_VALUE,NEW_VALUE,DATE,STATE,STATUS
```

**Tracked Fields**:
- Company name
- Company status
- Authorized capital
- Paid-up capital
- Registered office address
- Principal business activity

### 3. Web Enrichment

**Purpose**: Augment company data with public web sources.

**Implementation Note**: Current implementation uses simulated enrichment with production-ready code structure. In production, this would connect to actual APIs and web sources.

**Data Sources**:
- ZaubaCorp: Director information, company type
- GST Portal: GSTIN, GST status
- MCA API: Compliance status, filing dates

**Enriched Fields**:
- Industry classification
- Sector categorization
- Director names
- GSTIN number
- Compliance status
- Source URLs

**Output**:
- `data/enriched/enriched_companies_<date>.csv`
- `data/enriched/current_enriched.csv`
- HTML report with enrichment summary

### 4. AI Insights

**Purpose**: Generate automated summaries and handle natural language queries.

**Daily Summary Generation**:
- Analyzes change logs
- Produces text and JSON summaries
- Highlights key statistics and trends

**Sample Summary Output**:
```
=== MCA Daily Change Summary ===
Date: 20251019

Overall Statistics:
- New Incorporations: 8
- Deregistrations: 0
- Field Updates: 10000
- Total Changes: 10008

Top States by Changes:
  - Gujarat: 3
  - Maharashtra: 3
  - Delhi: 2

Most Updated Fields:
  - AUTHORIZED_CAPITAL: 5000
  - PAIDUP_CAPITAL: 5000
```

**Chatbot Capabilities**:
The AI chatbot can understand and respond to queries like:
- "Show new incorporations in Maharashtra"
- "How many companies were struck off last month?"
- "List companies with authorized capital above 10 lakh"
- "What is the status distribution of companies?"

**Implementation**: Uses rule-based natural language processing to parse queries and execute corresponding database operations.

### 5. Database Management

**Schema**:

**companies** table:
- cin (PRIMARY KEY)
- company_name
- company_class
- company_status
- authorized_capital
- paidup_capital
- state
- date_of_incorporation
- And other fields...

**changes** table:
- id (PRIMARY KEY)
- cin (FOREIGN KEY)
- change_type
- field_changed
- old_value
- new_value
- date
- state

**enriched_data** table:
- cin (PRIMARY KEY, FOREIGN KEY)
- industry
- sector
- directors
- gstin
- compliance_status
- source
- enrichment_date

## API Documentation

### Base URL
```
http://localhost:5000
```

### Endpoints

#### 1. Search Company
```
GET /api/search_company?query=<search_term>
```

**Parameters**:
- `query` (required): CIN or company name
- `state` (optional): Filter by state
- `status` (optional): Filter by company status

**Example**:
```bash
curl "http://localhost:5000/api/search_company?query=TECH"
```

**Response**:
```json
{
  "total": 5,
  "companies": [...]
}
```

#### 2. Get Company Details
```
GET /api/company/<cin>
```

**Example**:
```bash
curl "http://localhost:5000/api/company/U74900MH2009PTC123456"
```

**Response**:
```json
{
  "company": {
    "cin": "U74900MH2009PTC123456",
    "company_name": "TECH SOLUTIONS PRIVATE LIMITED",
    ...
  }
}
```

#### 3. Get Recent Changes
```
GET /api/changes?date=<YYYYMMDD>&type=<change_type>
```

**Parameters**:
- `date` (optional): Specific date
- `type` (optional): NEW_INCORPORATION, DEREGISTRATION, or FIELD_UPDATE

**Example**:
```bash
curl "http://localhost:5000/api/changes?type=NEW_INCORPORATION"
```

#### 4. Get Statistics
```
GET /api/stats
```

**Example**:
```bash
curl "http://localhost:5000/api/stats"
```

**Response**:
```json
{
  "total_companies": 5000,
  "active_companies": 4500,
  "states_covered": 5,
  "total_changes": 10052,
  "status_distribution": [...],
  "state_distribution": [...]
}
```

#### 5. Get Companies by State
```
GET /api/state/<state_name>
```

**Example**:
```bash
curl "http://localhost:5000/api/state/Maharashtra"
```

### Testing with Postman

1. Import the base URL: `http://localhost:5000`
2. Create requests for each endpoint
3. Add query parameters as needed
4. Set headers: `Content-Type: application/json`

## Technical Details

### Data Quality Score

Each company record receives a data quality score (0-1) based on completeness:
```python
score = (non_null_fields) / (total_fields)
```

Records with score < 0.5 are flagged for review.

### Change Detection Algorithm

The system uses a three-phase approach:

1. **Set Operations**: Identify new and removed CINs using set difference
2. **Field Comparison**: For common CINs, compare tracked fields
3. **Logging**: Record all changes with metadata (date, old/new values)

### CIN Structure Parsing

Corporate Identification Number format:
```
U74900MH2009PTC123456
│ │     │  │   │  │
│ │     │  │   │  └─ Sequence (6 digits)
│ │     │  │   └──── Company Type (PTC/PLC/OPC)
│ │     │  └──────── Year (4 digits)
│ │     └─────────── State Code (2 letters)
│ └───────────────── Industry Code (5 digits)
└─────────────────── Listing Status (L/U)
```

This structure is parsed in `web_enrichment.py` to extract metadata.

### Performance Considerations

- **Batch Processing**: Web enrichment uses ThreadPoolExecutor for parallel requests
- **Caching**: Enrichment results are cached to avoid redundant API calls
- **Indexing**: Database indexes on CIN, state, and status for fast queries
- **Rate Limiting**: Web requests include delays to respect source rate limits

## Troubleshooting

### Issue: pandas Installation Fails on Mac M1

**Solution**:
```bash
pip install --upgrade pip setuptools wheel
pip install pandas --no-build-isolation
```

Or use conda:
```bash
conda install -c conda-forge pandas
```

### Issue: Database Locked Error

**Cause**: Multiple processes accessing SQLite simultaneously

**Solution**:
- Stop all running instances (dashboard, API)
- Restart one at a time
- Consider upgrading to PostgreSQL for production

### Issue: No Module Named 'src'

**Cause**: Python path not configured

**Solution**:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

Or run from project root:
```bash
cd /path/to/mca-insights-engine
python src/data_integration.py
```

### Issue: Streamlit Port Already in Use

**Solution**:
```bash
streamlit run app/streamlit_app.py --server.port 8502
```

### Issue: Change Logs Not Generated

**Cause**: Insufficient snapshots

**Solution**:
Ensure at least two snapshots exist in `data/snapshots/`:
```bash
ls data/snapshots/
```

If missing, run:
```bash
python src/data_integration.py
```

### Issue: Empty Enriched Dataset

**Cause**: No recent changes detected

**Solution**:
The enrichment module processes companies with recent changes. If no changes exist, it samples from the master dataset. Check `data/change_logs/` for change log files.

## Data Sources

This project uses data from:
- **MCA Company Master Data**: Available at [data.gov.in](https://data.gov.in/catalog/company-master-data)
- **Sample Data**: Generated using `generate_sample_data.py` for testing and demonstration

The enrichment module references but does not actually scrape from:
- ZaubaCorp
- GST Portal
- MCA21 Portal
- API Setu

In production, these would require API keys and appropriate access permissions.

## Future Enhancements

Potential improvements for production deployment:
1. Integration with actual MCA APIs using official keys
2. Real-time change detection using webhooks
3. PostgreSQL database for better concurrency
4. Docker containerization for easy deployment
5. Email/SMS alerts for critical changes
6. Advanced ML models for company risk scoring
7. Integration with financial databases for credit analysis
8. Historical trend analysis and forecasting

## License

This project is created for educational and demonstration purposes as part of the MCA Insights Engine assignment.

## Contact

For questions or issues related to this implementation, please refer to the assignment documentation or contact the project maintainer.

---

**Note**: This implementation includes simulated web enrichment functionality as permitted by the assignment guidelines. The code structure is production-ready and demonstrates the intended logic and integration patterns. In a production environment, actual API integrations and web scraping implementations would replace the simulated methods.
