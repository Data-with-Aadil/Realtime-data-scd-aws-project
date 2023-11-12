# Real-Time Data Processing POC with AWS, Docker, NiFi, and Snowflake

![image](https://github.com/Data-with-Aadil/Realtime-data-scd-aws-project/assets/131682034/1e72e816-75d7-44d2-9615-c261fa75b5d9)

## Project Overview

### Data Generation with Faker Python Library

The project kicks off with the generation of synthetic data using the Faker Python library. This simulated data serves as our testing ground for real-time processing capabilities.

### Real-Time Processing with NiFi

Our data flows into action through Apache NiFi, where real-time ingestion and transformation take place. NiFi is configured to connect to the Faker data source, and the processed data is efficiently written to an S3 bucket in CSV format, serving as a staging area before its journey into Snowflake.

### AWS EC2 Instance and Docker Compose

The entire system is hosted on an AWS EC2 instance, utilizing Docker for containerization. Docker Compose orchestrates the deployment of Zookeeper, NiFi, and other essential components, streamlining the setup process.

### Snowflake Integration for Data Warehousing

Snowflake, a cloud-based data warehousing platform, plays a pivotal role in storing and managing our processed data. Snowpipe, Snowflake's data ingestion service, handles delta loading into the `customer_raw` staging table, acting as the gateway to our structured data.

A Snowflake Stream is established on the production table (`customer`) to capture change data. This stream is crucial for implementing historical tracking in our Slowly Changing Dimension (SCD-2) approach. Additionally, Snowflake tasks are configured to run at one-minute intervals, automating the data movement from the staging table to both the production and historical tables.

## Streamlining Production with Task Automation

Automation is at the heart of our project. Scheduled tasks, running at one-minute intervals, ensure the smooth transition of data from the raw stage (`customer_raw`) to the production table (`customer`). Here, we implement Slowly Changing Dimension (SCD) methodologies:

- **SCD-1 (Type 1):** The production table (`customer`) maintains only the latest version of each record without historical tracking.

- **SCD-2 (Type 2):** A historical table (`customer_historical`) captures changes in data over time, enabling a comprehensive view of historical records.






