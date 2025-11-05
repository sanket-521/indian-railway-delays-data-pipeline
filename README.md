# 🚆 Indian Railway Delays Data Pipeline (AWS Data Lake Project)

### 👤 Author: Sanket Aba Adhav  
**Role:** Data Engineer  

---

## 🧠 Project Overview

This project implements an **end-to-end AWS Data Lake pipeline** to process and analyze **Indian Railway train delay data** using **AWS Glue, S3, and Athena**.  

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** and demonstrates modern cloud data engineering principles including ETL, data modeling, and serverless querying.

---

## 🏛️ Architecture Diagram

![Architecture Diagram](docs/architecture_diagram.png)  

*Figure 1: AWS Data Lake Architecture for Train Delay Analysis.*

---

## 🗂️ Data Flow Overview

| Layer | Zone | Description | Output |
|--------|------|-------------|---------|
| **Raw** | Bronze | Source CSV data from Indian Railways | `train_delays.csv` |
| **Staging** | Silver | Cleaned, standardized Parquet data | `train_delays_cleaned/` |
| **Curated** | Gold | Aggregated datasets for analytics | `avg_delay_per_train/`, `avg_delay_per_route/` |

---

## ⚙️ AWS Services Used

| Service | Purpose |
|----------|----------|
| **AWS S3** | Data lake storage (Raw → Staging → Curated) |
| **AWS Glue (PySpark)** | ETL transformation and aggregation |
| **AWS IAM** | Role-based access control for Glue and S3 |
| **AWS Glue Crawler** | Schema inference and Data Catalog registration |
| **AWS Glue Catalog** | Central metadata store for Athena |
| **AWS Athena** | Serverless SQL queries on curated data |

---

## 🧩 AWS Glue Jobs

### 🔹 Job 1: Raw → Staging (`transform_to_staging.py`)
**Goal:** Clean and standardize raw CSV data.  
**Steps:**
- Read data from `s3://indian-railway-delays-data-pipeline-sanket/raw/train_delays.csv`
- Convert timestamps, calculate arrival/departure delays
- Derive columns: `day_of_week`, `status`, `avg_speed_kmph`
- Save as **Parquet** to:
  ```
  s3://indian-railway-delays-data-pipeline-sanket/staging/train_delays_cleaned/
  ```

---

### 🔹 Job 2: Staging → Curated (`load_to_curated.py`)
**Goal:** Create analytical datasets.  
**Steps:**
- Read staging Parquet data  
- Aggregate average delays per train and per route  
- Write results to curated zone:
  ```
  s3://indian-railway-delays-data-pipeline-sanket/curated/avg_delay_per_train/
  s3://indian-railway-delays-data-pipeline-sanket/curated/avg_delay_per_route/
  ```

---

## 🖋️ Glue Crawlers Configuration

| Crawler | Path | Output Database | Tables Created |
|----------|------|------------------|----------------|
| `raw_train_data_crawler` | `s3://.../raw/` | `railwaysdb` | `raw` |
| `curated_train_data_crawler` | `s3://.../curated/` | `railwaysdb` | `avg_delay_per_train`, `avg_delay_per_route` |

---

## 🧮 Athena Queries

Once the curated crawler registers the schema in Glue Catalog, you can query data directly in **Athena**.

```sql
-- Top 10 most delayed trains
SELECT
  train_no, train_name, avg_arrival_delay, avg_departure_delay
FROM avg_delay_per_train
ORDER BY avg_arrival_delay DESC
LIMIT 10;
```

```sql
-- Most delayed routes
SELECT
  source_station_code, destination_station_code, avg_delay_route
FROM avg_delay_per_route
ORDER BY avg_delay_route DESC
LIMIT 10;
```

---

## 📊 Athena Query Output

Below is a sample query result from the `avg_delay_per_train` table:

![Athena Output](docs/athena_output_sample.png)  
*Figure 2: Athena query showing top delayed trains from curated dataset.*

---

## 📈 Example Insights

| Metric | Result |
|---------|---------|
| 🚉 Most Delayed Train | Rajdhani Express — 24.5 min delay |
| 🕒 Average Delay (All Trains) | 14.2 min |
| 🛍️ Route with Max Delay | NDLS → BCT |
| 📆 Worst Day | Monday |

---

## 🧮 Tech Stack

- **Language:** Python (PySpark)
- **Storage:** AWS S3 (Raw, Staging, Curated)
- **ETL Tool:** AWS Glue
- **Query Engine:** AWS Athena
- **Metadata Store:** AWS Glue Data Catalog
- **Permissions:** AWS IAM

---

## 🌟 Highlights

✅ Implemented multi-zone S3 data lake (Raw, Staging, Curated)  
✅ Created parameterized Glue ETL jobs  
✅ Automated schema registration using Glue Crawlers  
✅ Queried Parquet datasets via Athena  
✅ Cloud-native & serverless data pipeline  

---

## 🪴 Project Folder Structure

```
indian-railway-delays-data-pipeline/
│
├── glue_jobs/
│   ├── transform_to_staging.py
│   └── load_to_curated.py
│
├── aws_infra/
│   ├── iam_roles_setup.txt
│   ├── crawlers_config.md
│   ├── glue_job_parameters.txt
│   └── bucket_structure.txt
│
├── athena_queries/
│   ├── top_delayed_trains.sql
│   ├── most_delayed_routes.sql
│   └── avg_delay_by_day.sql
│
├── docs/
│   ├── architecture_diagram.png
│   ├── athena_output_sample.png
│   └── project_summary.pdf
│
├── README.md
```

---

## 🚀 Future Enhancements

- Automate pipeline using **AWS Glue Workflows / Airflow**
- Add **data validation checks** in staging job
- Integrate with **QuickSight** for dashboards
- Schedule periodic updates via **CloudWatch Triggers**

---

## 👨‍💻 Author

**Sanket Aba Adhav**  
_Data Engineer | AWS & PySpark Enthusiast_  
📧 [sankettadhav2004@gmail.com]  
🔗 [LinkedIn](www.linkedin.com/in/sanket-adhav-279023257)  
🔗 [GitHub](https://github.com/sanket-521/indian-railway-delays-data-pipeline)

---

> *“Turning raw data into reliable insights using scalable AWS data pipelines.”*

