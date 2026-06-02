# News Scraping Pipeline with Apache Airflow & DVC

An automated MLOps pipeline that scrapes, cleans, and versions news article metadata from Dawn and BBC using Apache Airflow for orchestration and DVC for data versioning.

---

## Overview

This project builds an end-to-end data pipeline as part of an MLOps module at Manchester Metropolitan University. The pipeline extracts article titles and descriptions from news websites, applies NLP-based text cleaning, and outputs a versioned CSV — with the full workflow automated via Apache Airflow and data tracked via DVC.

---

## Pipeline Architecture

```
Extract (Scrape URLs + metadata)
    ↓
Transform (Clean + normalise text)
    ↓
Load (Export to CSV)
    ↓
Version (DVC add + push)
    ↓
Commit (Git push)
```

Each stage is a discrete Airflow task, with task dependencies enforced via the `>>` operator.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Airflow | Pipeline orchestration and scheduling |
| DVC | Data versioning and remote storage |
| BeautifulSoup | HTML parsing and scraping |
| NLTK | Text preprocessing (tokenisation, stopword removal, lemmatisation) |
| pandas | Tabular data handling and CSV export |

---

## NLP Preprocessing Steps

The `transform` stage applies the following to each title and description:

1. HTML tag removal via regex
2. Lowercasing
3. Tokenisation (`nltk.word_tokenize`)
4. Non-alphabetic token removal
5. Stopword removal (English)
6. Lemmatisation (`WordNetLemmatizer`)

---

## Project Structure

```
.
├── pipeline.py          # Airflow DAG definition + ETL functions
├── requirements.txt     # Python dependencies
├── webscrapped.csv      # Output data (DVC-tracked, not committed to Git)
├── webscrapped.csv.dvc  # DVC pointer file
└── README.md
```

---

## Setup

### Prerequisites
- Python 3.8+
- Apache Airflow 2.x
- DVC

### Install dependencies

```bash
pip install -r requirements.txt
```

### NLTK downloads

```python
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
```

### Run locally (without Airflow)

```bash
python pipeline.py
```

### Deploy to Airflow

Place `pipeline.py` in your Airflow `dags/` directory. The DAG is scheduled to run daily.

---

## Known Limitations

- Global list state means Airflow task retries will append duplicate records — a production version would use XCom or a database for inter-task state
- Scraping relies on Dawn's current HTML class structure (`story__link`) and will break if the site changes
- BBC scraping is imported as a source but not yet implemented
- `display()` in the load function is Jupyter-specific and will error in plain script execution outside a notebook environment

---

## Skills Demonstrated

- Airflow DAG authoring with `PythonOperator` and `BashOperator`
- Task dependency management
- Data versioning workflow with DVC
- NLP text preprocessing pipeline
- Web scraping with requests and BeautifulSoup
