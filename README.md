# Data Warehouse in AWS Redshift

This is the third project in Udacity's Data Engineering Nanodegree. In this project, music streaming app, **Sparkify**, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Datasets

Data is located in a public S3 bucket. Here are the S3 links for each:

### Song Data

````s3://udacity-dend/song_data````

### Log Data 

````s3://udacity-dend/log_data````

### Log Data json path

````s3://udacity-dend/log_json_path.json````

Log data json path is used to parse the JSON source data when copying data from S3 buckets to Redshift.

