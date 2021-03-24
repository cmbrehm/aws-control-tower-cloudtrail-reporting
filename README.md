# ????

One of the main features of AWS Control Tower is to automate the collection of logs from all dependent accounts across all regions into one single S3 bucket.

This project is an attempt to make those logs useful by utilizing AWS Glue to repartition and convert to Parquet format, so that AWS Athena or like tools can readily query them.

## Installation
AWS SAM is the best way to deploy bc it handles inlining the Glue job.

- Install AWS SAM
- Create your own S3 bucket in the CT Logs account (SAM won't be able to do this because the CT guardrail currently prevents adding a bucket policy to a bucket)
- Run command below
```
sam deploy --s3-bucket sam-cli-bucket-123412341234-us-east-1 --stack-name aam-glue --parameter-overrides 'S3BucketName=aws-controltower-logs-123412341234-us-east-1 S3LogsUrl=s3://aws-controltower-logs-123412341234-us-east-1/o-s45wxfxh7h/AWSLogs/'
```
