# AWS_DE_ENG_Proj_2
AWS Data Engineer Nanodegree Project 2: Create a Redshift Data Warehouse

## Introduction
The Data Warehouse project is part of the Data Engineering with AWS Nanodegree program. In this project, we work with a fictional music streaming company called Sparkify. The company has collected a large amount of data about their users, songs, and user activity logs. 
With the files in this Repo you can build an ETL (Extract, Transform, Load) pipeline that extracts the data from Amazon S3, stages it in Amazon Redshift, and transforms it into a set of dimensional tables for analysis.

The main goal of this project is to create a cloud-based data warehouse that allows Sparkify's analytics team to easily query and analyze the data to gain insights into user behavior, song preferences, and other business metrics. By building this data warehouse, you will enable the company to make data-driven decisions and improve their services.

## Redshift Architecture 
The goal of the project is to create a Redshift Architecture that stores data in a way that is optimized for analytical queries and visual insights:

<img height="400" width="600" alt="image" src="https://github.com/user-attachments/assets/afe34cc4-c63d-4aba-82d4-9d66335eb584">

## Visual Overview of the S3 to Redshift Architecture for Sparkify
<img height="400" width="600" alt="image" src="https://github.com/user-attachments/assets/29b9cb23-45c7-4891-a783-2a2191c60c1b">

## Star Schema Overview
Using the song and log data found in Amazon S3 the goal is to create the final STAR schema tables below in Amazon Redshift:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/790a26ae-7b69-413f-b64c-e868dd25845c">

## Project Setup Instructions
1. Create a cluster in Amazon Redshift. To quickly download the full songs dataset you will need an 8 node cluser with dc2.large instances. For optimal performance Redshift cluster should also be located in the 'us-west-2' AWS
   region where the song-data S3 bucket is located.
3. In the dwh.cfg file you need to change the CLUSTER and IAM_ROLE information for your IAM_ROLE and CLUSTER information.
4. Download the files to your local CPU or a cloud based IDE and run create_tables.py and etl.py
5. Now your data is ready in a final STAR Schema data model for high performance analytics and ad hoc queries

## Files Explanation
- `sql_queries.py`: Create SQL queries to perform all stages of the project. Queries are loaded into the other files as lists of python strings.
- `create_Table.py`: Checks for existing of tables with the desired names in the DB and drops them. Then creates 7 template tables for staging and final data insertion.
- `etl.py`: Copies data from Udacity S3 buckets to the Redshift Staging tables. Inserts data from the Staging tables into the final STAR Schema tables.

## References
This project referenced Udacity course material, the Udacity Knowledge portal, and Udacity GPT as resources
