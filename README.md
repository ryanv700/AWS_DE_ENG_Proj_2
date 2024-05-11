# AWS_DE_ENG_Proj_2
AWS Data Engineer Nanodegree Project 2: Create a Redshift Data Warehouse

## Introduction
The Data Warehouse project is part of the Data Engineering with AWS Nanodegree program. In this project, we work with a fictional music streaming company called Sparkify. The company has collected a large amount of data about their users, songs, and user activity logs. 
With the files in this Repo you can build an ETL (Extract, Transform, Load) pipeline that extracts the data from Amazon S3, stages it in Amazon Redshift, and transforms it into a set of dimensional tables for analysis.

The main goal of this project is to create a cloud-based data warehouse that allows Sparkify's analytics team to easily query and analyze the data to gain insights into user behavior, song preferences, and other business metrics. By building this data warehouse, you will enable the company to make data-driven decisions and improve their services.

## Project Setup Instructions
1. Create a cluster in Amazon Redshift. To quickly download the full songs dataset you will need an 8 node cluser with dc2.large instances.
2. In the dwh.cfg file you need to change the CLUSTER and IAM_ROLE information for your IAM_ROLE and CLUSTER information.
3. Download the files to your local CPU or a cloud based IDE and run create_tables.py and etl.py
4. Now your data is ready in a final STAR Schema data model for high performance analytics and ad hoc queries

## Files Explanation
- `sql_queries.py`: Create SQL queries to perform all stages of the project. Queries are loaded into the other files as lists of python strings.
- `create_Table.py`: Checks for existing of tables with the desired names in the DB and drops them. Then creates 7 template tables for staging and final data insertion.
- `etl.py`: Copies data from Udacity S3 buckets to the Redshift Staging tables. Inserts data from the Staging tables into the final STAR Schema tables.

## References
This project referenced Udacity course material, the Udacity Knowledge portal, and Udacity GPT as references
