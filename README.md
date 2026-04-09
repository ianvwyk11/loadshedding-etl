# 🔌 South African Load Shedding ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-red?logo=apacheairflow)
![SQLite](https://img.shields.io/badge/SQLite-3.x-lightblue?logo=sqlite)
![License](https://img.shields.io/badge/License-MIT-green)
![CI](https://github.com/YOUR_USERNAME/loadshedding-etl/actions/workflows/ci.yml/badge.svg)

An end-to-end data engineering pipeline that **extracts**, **transforms**, and **loads** South African load shedding schedule data from the EskomSePush API into a structured analytical database — orchestrated with Apache Airflow and scheduled to run daily.

---

## 📐 Architecture

```
EskomSePush API
      │
      ▼
 [Extract]  ──▶  data/raw/       (raw JSON)
      │
      ▼
 [Transform] ──▶  data/processed/ (clean CSVs)
      │
      ▼
 [Load]      ──▶  SQLite DB       (star schema)
      │
      ▼
 [Airflow DAG] ── scheduled daily
```

---

## 🗂️ Project Structure

```
loadshedding_etl/
├── etl/
│   ├── extract.py        # API ingestion
│   ├── transform.py      # Data cleaning & normalization
│   └── load.py           # Database loading
├── dags/
│   └── loadshedding_dag.py  # Airflow DAG definition
├── sql/
│   ├── create_tables.sql    # Schema definition
│   └── analysis_queries.sql # Analytical SQL queries
├── data/
│   ├── raw/              # Raw API responses (gitignored)
│   └── processed/        # Cleaned CSVs (gitignored)
├── notebooks/
│   └── exploration.ipynb # EDA & findings
├── tests/
│   └── test_transform.py # Unit tests
├── .github/workflows/
│   └── ci.yml            # GitHub Actions CI
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚙️ Setup

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/loadshedding-etl.git
cd loadshedding-etl
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Linux/macOS
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
# Add your EskomSePush API key to .env
```

### 5. Initialise the database
```bash
python -m etl.load --init
```

### 6. Run the pipeline manually
```bash
python -m etl.extract
python -m etl.transform
python -m etl.load
```

---

## 🗄️ Database Schema

The pipeline loads data into a **star schema**:

| Table | Type | Description |
|---|---|---|
| `fact_outages` | Fact | One row per scheduled outage slot |
| `dim_area` | Dimension | Area name, region, municipality |
| `dim_stage` | Dimension | Load shedding stage (1–8) |
| `dim_date` | Dimension | Date attributes for time-series analysis |

---

## 📊 Sample Insights

- Most frequently affected municipalities by outage hours
- Peak outage time slots nationally
- Stage escalation trends over time
- Average outage duration by region

---

## 🔄 Orchestration

The Airflow DAG (`dags/loadshedding_dag.py`) runs daily at 06:00 SAST and executes the full Extract → Transform → Load sequence with built-in retry logic and failure alerting.

---

## 🧪 Running Tests

```bash
pytest tests/ -v
```

---

## 📋 Data Source

Data is sourced from the [EskomSePush API](https://eskomsepush.gumroad.com/l/api).  
Free tier provides 50 API calls/day — sufficient for daily scheduled runs.

---

## 👤 Author

**Adriaan van Wyk**  
IBM Certified Data Engineer | IBM Certified ML Professional  
[LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [GitHub](https://github.com/YOUR_USERNAME)
