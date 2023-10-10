## Overview

The goal of the project is to learn 
1. 3-hop architecture to progressively transform data
2. Testing with Great Expectation
3. Implement Factory Pattern

Basically, it pulls order data from S3 bucket. Data is fake by a Docker container which generate data to push to Minio. Data is landed and transform through 3 layers by Spark.


### Data Architecture

<img src="image/dbt_project.png"/>


1. Extract customer data from OLTP database and load into to the data warehouse. Order data is pulled from S3 bucket

2. Implement dimensional approach with DBT

3. Serve data with Metabase

4. Orchestrate data pipeline with Airflow

<img src="image/dag.png"/>

## Prerequisites

Directions or anything needed before running the project.

- Github
- Terraform
- AWS Account
- AWS CLI installed and configured
- Docker with at least 4GB of RAM and Docker Compose v1.27.0 or later

## How to Run This Project

1. git clone https://github.com/trisdoan/dbt_project_1.git
2. cd dbt_project_1
3. make tf-init
4. make infra-up
5. make cloud-airflow (to port forward airflow on cloud to local machine)
6. go to http://localhost:8080/ to trigger dag
7. make cloud-metabase (to port forward metabase on cloud to local machine)
8. make infra-down to destroy data component

## Lessons Learned

I learned so many things during building this project

1. 

Reference
1. Machado, J. K. (2022, March 18). End-to-end data engineering project - batch edition. Start Data Engineering, from https://www.startdataengineering.com/post/data-engineering-project-e2e/

## Contact

Please feel free to contact me if you have any questions at: 
1. Linkedln: www.linkedin.com/in/trisdoan
2. Email: doanminhtri8183@gmail.com
