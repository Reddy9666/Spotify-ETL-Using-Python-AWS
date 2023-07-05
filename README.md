# Spotify-ETL-Pipeline

### Introduction
In this project, I tried to build ETL(Extract, Transform, Load) pipeline using the Spotify API on AWS, I retrieved data from Spotify API, Transform it to a desired format, and loaded it into AWS data Store.

### Architecture



### Services Used

1. **S3(simple storage service):** Amazon S3 is a s highly scalable object storage service that can store and retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups, and static website files.

2. **AWS Lambda:** AWS Lambda is a serverless computing service that lets you run your code without managing servers. you can use lambda to run code in response to events like changes in S3, DynamoDB, or other AWS services.

3. **Cloud watch:** Amazon Cloud watch is a monitoring service for AWS resources and the applications you run on them. you can use Cloudwatch to collect and track metrics, collect and monitor log files, and set alarms.

4. **Glue Crawler:** AWS glue crawler is a fully managed service that automatically crawls your data sources, identifies data formats, and infers schemes to create an AWS Glue Data Catalog

5. **Data Catalog:** AWS Glue data catalog is a fully managed metadata repository that makes it easier to discover and manage data in aws you can use the glue data catalog with other AWS services such as Athena.

6. **Amazon Athena:** Amazon Athena is an interactive query service that makes it easy to analyze data in amazon s3 using standard SQL you can use Athena to analyze data in your glue data catalog or in other S3 bucket

### Dataset/API
 This API contains information about music, artists, albums, and songs extracted from the Spotify API

### Project Execution Flow
Extract Data from API -> Lmbda Trigger ( every1 hour) -. Run Extract code -> store Raw data -> Trigger Transformation function -> Transform data and load it -> query using Athena
