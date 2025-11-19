Project Overview
This document tracks our progress in building an end-to-end AWS data pipeline for real estate price prediction using Zillow housing data and US Census demographics.

Built a cloud-based data pipeline that combines housing prices with neighborhood demographics. We processed 2.2 million properties, filtered to 300 samples across MA, CA, and NY, and joined them with census data. Now we can analyze how factors like income, education, and unemployment affect home prices. It's like building a mini-Zillow prediction system using AWS!


Project Objective
Build a cloud-based data pipeline that integrates real estate listings (2.2M properties) with demographic data to analyze how factors like income and education affect housing prices across Massachusetts, California, and New York.


AWS Services Used
•	Amazon S3 - Data lake storage (raw and processed layers)
•	AWS Lambda - Serverless data ingestion functions
•	AWS Glue - ETL job for filtering, cleaning, and enrichment
•	Amazon Athena - SQL queries on processed data
•	Amazon QuickSight - Data visualization (optional)

