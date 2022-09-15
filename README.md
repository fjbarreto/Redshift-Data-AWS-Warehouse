# Data Warehouse in AWS Redshift

This is the third project in Udacity's Data Engineering Nanodegree. In this project, music streaming app, **Sparkify**, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in **AWS S3** service, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Datasets

Data is located in a public S3 bucket. Here are the S3 links for each:

### Song Data

````s3://udacity-dend/song_data````

### Log Data 

````s3://udacity-dend/log_data````

### Log Data json path

````s3://udacity-dend/log_json_path.json````

Log data json path is used to parse the JSON source data when copying data from **S3** buckets to **Redshift**.

## Schema Design

First we need two tables that will stage data from JSON files to Redshift. 
**Staging_songs** table will be used to load song data and **Staging_events** will be used to stage log data.

Then using both staging tables we populate the following star schema with one fact table (songplays) and 4 dimension tables (artists, songs, users and time) is used. This schema design is optimized for songplay analyses.
![schema](https://user-images.githubusercontent.com/97537153/190182310-2b0d0aa9-813d-4a05-889f-1fb20be501bb.png)

## Files

• **sql_queries.py**: Python script with ````COPY````, ````CREATE````, ````DROP```` and ````INSERT```` statements needed to load all data into the Redshift Cluster.

• **create_tables.py**: This script will ````DROP Table IF EXISTS```` and re-create new tables.

• **etl.py**: This script executes `````COPY```` statements to staging tables, transforms and loads data into the tables in the schema. 

## Cluster Configurations
 
1. First you must create an ````IAM role```` with an attached policy called **AmazonS3ReadOnlyAccess**
2. Then naviagte to EC2 service where you will create a security group. A security group will act as firewall for your Redshift cluster to control inbound and outbound traffic. Create the security group and add the following inbound rules:
![inbound rules](https://user-images.githubusercontent.com/97537153/190350343-57420d2c-796f-40ed-a667-629d149f236f.png)
---
**NOTE**

Using ````0.0.0.0/0```` is not recommended for anything other than demonstration purposes because it allows access from any computer on the internet. 

---
3. Create redshift cluster with your preferences and make sure you attach the IAM role and security group just created so you can access it. 
4. Fill the **dwh.config** file with all the information of your IAM role and redshift cluster.


