import boto3
import json
import os

topic = os.environ['SNS_TOPIC']
def lambda_handler(event, context):
    print (json.dumps(event))
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    rec = event['Records'][0]
    ## for-each?
    message=s3.get_object(
        Bucket = rec['s3']['bucket']['name'],
        Key = rec['s3']['object']['key']
    )
    rawm = message['Body'].read().decode('utf-8')
    sns.publish (
        TopicArn = topic,
        Message = rawm
    )
    print(message);
    return {
        "statusCode":200,
        "body": rawm
    }
