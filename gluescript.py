import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
import boto3
import datetime

glueContext = GlueContext(SparkContext.getOrCreate())
args = getResolvedOptions(sys.argv, ['JOB_NAME','source_bucket','TempDir','dest_bucket'])
sourceBucket = args.get('source_bucket')
tempBucketUrl = args.get('TempDir')
destBucket = args.get('dest_bucket')
yesterday = datetime.datetime.now()-datetime.timedelta(days=1)
sourceDate = yesterday.strftime("%Y/%m/%d/")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3client=boto3.client('s3')

#functions
flatten=lambda t:[item for sublist in t for item in sublist]
s3List=lambda pref='': s3client.list_objects_v2(Bucket=sourceBucket, Delimiter='/', Prefix=pref)

orgResp=s3List()
orgPrefix='{}AWSLogs/'.format(orgResp['CommonPrefixes'][0]['Prefix'])
# assuming one org cause Control Tower

accountsResp=s3List(orgPrefix)
accounts=[p['Prefix'][-13:-1] for p in accountsResp['CommonPrefixes']]
# now we have all accounts

accountDict = { acct:s3List("{}{}/CloudTrail/".format(orgPrefix,acct)) for acct in accounts }
regionList=list(accountDict.values())
regionPrefixes = flatten([ r['CommonPrefixes'] for r in regionList ])
## get the regions

folders=[p['Prefix']+sourceDate for p in regionPrefixes]
#append the date
data = glueContext.create_dynamic_frame.from_options(connection_type='s3', format="json", format_options={"jsonPath":"$.Records[*]"},connection_options={"paths":["s3://{}/{}".format(sourceBucket,f) for f in folders], "recurse":True, "compressionType":"gzip"}, transformation_ctx="datasource")

applymapping1 = ApplyMapping.apply(frame = data, mappings = [("eventversion", "string", "eventversion", "string"), ("useridentity", "struct", "useridentity", "struct"), ("eventtime", "string", "eventtime", "string"), ("eventsource", "string", "eventsource", "string"), ("eventname", "string", "eventname", "string"), ("awsregion", "string", "awsregion", "string"), ("sourceipaddress", "string", "sourceipaddress", "string"), ("useragent", "string", "useragent", "string"), ("requestid", "string", "requestid", "string"), ("eventid", "string", "eventid", "string"), ("eventtype", "string", "eventtype", "string"), ("recipientaccountid", "string", "recipientaccountid", "string"), ("errorcode", "string", "errorcode", "string"), ("errormessage", "string", "errormessage", "string"), ("sharedeventid", "string", "sharedeventid", "string"), ("apiversion", "string", "apiversion", "string"), ("readonly", "boolean", "readonly", "boolean"), ("managementevent", "boolean", "managementevent", "boolean"), ("eventcategory", "string", "eventcategory", "string"), ("vpcendpointid", "string", "vpcendpointid", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1")
# crush the struct mapping down.
relationalize0 = Relationalize.apply(frame = applymapping1, name="dfcroot", staging_path=tempBucketUrl, transformation_ctx="relationalize0")
relationalize1 = relationalize0.select("dfcroot")

#add the partition info back in
sourceDtArr = sourceDate.split('/')
relationalize11 = relationalize1.toDF().select('*',lit(sourceDtArr[0]).alias("year"),lit(sourceDtArr[1]).alias("month"),lit(sourceDtArr[2]).alias("day"))
relationalize2= DynamicFrame.fromDF(relationalize11, glueContext,"relationalize2")

#write
glueContext.write_dynamic_frame.from_options(frame = relationalize2, connection_type = "s3", connection_options = {"path": "s3://"+destBucket+"/cloudtrail/", "partitionKeys":["year","month","day"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
