import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'destbucket', 'database', 'srctable'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "audit", table_name = "raw_awslogs", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['database'], table_name = args['srctable'], transformation_ctx = "datasource0")## @type: Relationalize
## @type: ApplyMapping
## @args: [mapping = [("eventversion", "string", "eventversion", "string"), ("useridentity", "struct", "useridentity", "struct"), ("eventtime", "string", "eventtime", "string"), ("eventsource", "string", "eventsource", "string"), ("eventname", "string", "eventname", "string"), ("awsregion", "string", "awsregion", "string"), ("sourceipaddress", "string", "sourceipaddress", "string"), ("useragent", "string", "useragent", "string"), ("requestid", "string", "requestid", "string"), ("eventid", "string", "eventid", "string"), ("eventtype", "string", "eventtype", "string"), ("recipientaccountid", "string", "recipientaccountid", "string"), ("errorcode", "string", "errorcode", "string"), ("errormessage", "string", "errormessage", "string"), ("sharedeventid", "string", "sharedeventid", "string"), ("apiversion", "string", "apiversion", "string"), ("readonly", "boolean", "readonly", "boolean"), ("managementevent", "boolean", "managementevent", "boolean"), ("eventcategory", "string", "eventcategory", "string"), ("vpcendpointid", "string", "vpcendpointid", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("eventversion", "string", "eventversion", "string"), ("useridentity", "struct", "useridentity", "struct"), ("eventtime", "string", "eventtime", "string"), ("eventsource", "string", "eventsource", "string"), ("eventname", "string", "eventname", "string"), ("awsregion", "string", "awsregion", "string"), ("sourceipaddress", "string", "sourceipaddress", "string"), ("useragent", "string", "useragent", "string"), ("requestid", "string", "requestid", "string"), ("eventid", "string", "eventid", "string"), ("eventtype", "string", "eventtype", "string"), ("recipientaccountid", "string", "recipientaccountid", "string"), ("errorcode", "string", "errorcode", "string"), ("errormessage", "string", "errormessage", "string"), ("sharedeventid", "string", "sharedeventid", "string"), ("apiversion", "string", "apiversion", "string"), ("readonly", "boolean", "readonly", "boolean"), ("managementevent", "boolean", "managementevent", "boolean"), ("eventcategory", "string", "eventcategory", "string"), ("vpcendpointid", "string", "vpcendpointid", "string"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1")
## @args: [staging_path = "<staging_path>", name = "<name>", transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
relationalize0 = Relationalize.apply(frame = applymapping1, staging_path="s3://"+args["destbucket"]+"/tmp/", name="dfcroot", transformation_ctx="relationalize0")
relationalize1 = relationalize0.select("dfcroot")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://cloudtrail-parquet-123443713593-us-east-1"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = relationalize1, connection_type = "s3", connection_options = {"path": "s3://"+args["destbucket"]+"/cloudtrail/", "partitionKeys":["year","month","day"]}, format = "parquet", transformation_ctx = "datasink4")
job.commit()
