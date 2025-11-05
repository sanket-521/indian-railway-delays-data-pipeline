# AWS Glue Crawlers Configuration

## 1. Raw Layer Crawler
- **Name:** raw_train_data_crawler
- **S3 Path:** s3://indian-railway-delays-data-pipeline-sanket/raw/
- **Output Database:** indian_railway_raw_db
- **Table Created:** train_delays_raw
- **Run Schedule:** On Demand

## 2. Curated Layer Crawler
- **Name:** curated_train_data_crawler
- **S3 Path:** s3://indian-railway-delays-data-pipeline-sanket/curated/
- **Output Database:** indian_railway_curated_db
- **Tables Created:**
  - avg_delay_per_train
  - avg_delay_per_route
- **Run Schedule:** On Demand

## Purpose:
Crawlers automatically detect schema changes and register data in AWS Glue Catalog, making it queryable via Athena.
