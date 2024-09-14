# GCP-Data-Pipeline-Project
Built an end-to-end ETL data pipeline on Google Cloud Platform using the Myntra dataset from Kaggle. Tested and orchestrated the pipelines using Docker and Apache Airflow.

![Screenshot (493)](https://github.com/user-attachments/assets/f2205ca2-3f9b-4654-b66c-9fa6f9e21852)


# Project Overview :
This project involves building an end-to-end data pipeline using Google Cloud Platform (GCP). The pipeline is designed to handle data ingestion, transformation, and loading processes with the Myntra dataset from Kaggle. The project leverages GCP services such as Google Cloud Storage and BigQuery, with Docker for containerization and Apache Airflow for orchestration.



## 1. Uploading and Initial Pipeline :
Manually uploaded the Myntra CSV dataset to a Google Cloud Storage (GCS) bucket. The first pipeline was designed to extract the file from the GCS bucket, apply a custom schema, and load the data into a BigQuery table. The schema was predefined based on the dataset structure, ensuring correct data mapping in BigQuery.

## 2. Second Pipeline (ETL Pipeline) : 
The second pipeline performed the Extract, Transform, and Load (ETL) process. It extracted data from the BigQuery table, transformed, and then loaded the data into another BigQuery table.

Both pipelines were developed using Python and Pandas. The Google Cloud services involved were Google Cloud Storage (Nearline storage type) for storing the dataset and BigQuery for data warehousing.

## 3. Docker in (Virtual Machine) :
- Built two Docker images:
  - One image extracted files from the GCS bucket and loaded them into BigQuery table.
  - The second image handled the ETL process: extracting data from BigQuery table, transforming it, and loading it back into another BigQuery table.
  
- The Docker containers were run using `docker-compose.yml`, which allowed the execution of both images simultaneously. Running the containers automated the workflow, completing the entire process without manual intervention.

- Both images were tagged and pushed to Docker Hub.

### Docker Attributes :
Docker was installed on a virtual machine. The VM instance was launched using Ubuntu 20.04 LTS. with the following specifications:
- Disk Space : 10 GB
- Architecture : x86/64
- Machine Type : e2-medium (2 vCPUs, 4 GB memory), providing a cost-effective computing solution.


## 4. Airflow in (Cloud Composer) :
- Two separate DAGs were written in Airflow:
  - The first DAG was responsible for extracting files from the GCS bucket and loading them into BigQuery table.
  - The second DAG handled the ETL process, extracting data from BigQuery, transforming it, and loading it back into another table in BigQuery.

- These tasks were orchestrated and managed using the Airflow UI. Testing was conducted directly within the UI, ensuring smooth execution of all tasks.

### Airflow Attributes :
Cloud Composer 3 was used, featuring autoscaling, serverless infrastructure, and Airflow 2. The environment resources were configured as "small" and included with the following specifications:
  - 1 Scheduler: 0.5 vCPU, 2 GB memory, 1 GB storage
  - 1 DAG Processor: 1 vCPU, 2 GB memory, 1 GB storage
  - 1 Triggerer: 0.5 vCPU, 1 GB memory, 1 GB storage
  - 1 Web Server: 0.5 vCPU, 2 GB memory, 1 GB storage
  - Autoscaling between 1-3 workers, with 0.5 vCPU, 2 GB memory, and 10 GB storage per worker.

## 5. Identity & Access Management (IAM) :
IAM services were used to manage access permissions for the necessary Google Cloud operations.


## Languages & Services Used :
- Python
- Pandas
- Docker
- Apache Airflow
- Google Cloud Storage (GCS) Bucket
- Bigquery
- Virtual Machine
- Cloud Composer
- IAM
