import os
import boto3
import datetime

DATABASE = os.environ.get('DATABASE')
output = os.environ.get('OUTPUT_BUCKET')
TABLE = os.environ.get('TABLE_NAME')
WORKGROUP = os.environ.get('WORKGROUP')

def lambda_handler(event, context):
    t = datetime.date.today() - datetime.timedelta(days=1)
    query = (
        f'SELECT distinct "useridentity.principalid", eventsource, eventname, recipientaccountid '
        f'FROM "{TABLE}" where '
        f"year='{t.year}' and month='{str(t.month).zfill(2)}' and day='{str(t.day).zfill(2)}' "
        f"and eventname like 'Create%';"
    );
    client = boto3.client('athena')
    print (f"query is {query}")
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': f's3://{output}/',
        },
        WorkGroup=WORKGROUP
    )
    return response
