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

Technical Aspect : 

Big Data: We processed 2.2 MILLION properties!
Cloud Computing: Used Amazon's supercomputers (not our laptop)
Real APIs: Got live data from government databases
ETL Pipeline: Built a professional data processing system
Data Enrichment: Combined multiple sources intelligently

Project Objective 

Primary Goal 

Research Question: How do neighborhood demographic factors (income, education, employment) influence residential property prices in major metropolitan areas? 

For Home Buyers: 

Understand what drives pricing in different neighborhoods 

Make informed decisions about property value relative to area demographics 

Identify neighborhoods with strong fundamentals 

For Real Estate Investors: 

Discover undervalued markets with strong demographic indicators 

Predict price trends based on demographic changes 

Calculate risk-adjusted returns using neighborhood data 

3. Data Sources 

Data Source 1: Zillow Housing Data (CSV File) 

Source: Kaggle - USA Real Estate Dataset 
 URL: https://www.kaggle.com/datasets/ahmedshahriarsakib/usa-real-estate-dataset 
 Type: Static CSV file 

Coverage: All 50 U.S. states 

 Time Period: Recent listings (2020-2023) 

 Quality: Commercial-grade real estate data 

 

Data Source 2: U.S. Census Bureau API (JSON) 

Source: U.S. Census Bureau American Community Survey (ACS) 
 API Endpoint: https://api.census.gov/data/2021/acs/acs5 
 Type: REST API (real-time) 
 Format: JSON 
 Authentication: API key required 

 

Derived Metrics: 

College Educated Percentage = (Bachelor's + Master's) / Education Total × 100 

Unemployment Rate = Unemployed / Labor Force × 100 

Coverage: 90+ ZIP codes across target states 

Massachusetts: 21 ZIP codes (Boston metro) 

California: 35 ZIP codes (Los Angeles, San Francisco) 

New York: 30 ZIP codes (Manhattan, Queens, Brooklyn) 



Architecture Components 

1. Data Ingestion Layer 

AWS Lambda functions (serverless) 

2. Storage Layer 

AWS S3 (object storage) 

Separate buckets for RAW and PROCESSED data 

Organized folder structure 

3. Transformation Layer 

AWS Glue (managed Spark) 

Distributed data processing 

Automatic scaling 

4. Catalog Layer 

AWS Glue Data Catalog 

Metadata management 

Schema registry 

5. Analysis Layer 

AWS Athena (serverless SQL) 
