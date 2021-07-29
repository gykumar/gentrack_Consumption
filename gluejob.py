import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "gentrack-etl", table_name = "gentrack_raw_zone", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "gentrack-etl", table_name = "gentrack_raw_zone", transformation_ctx = "datasource0")

##DynamicFrame to DataFrame
df = datasource0.toDF()
##Get meter,date and sum of readings
rdd1=df.rdd.map(lambda x:(x[0],x[1],x[2]+x[3]+x[4]+x[5]+x[6]+x[7]+x[8]+x[9]+x[10]+x[11]+x[12]+x[13]+x[14]+x[15]+x[16]+x[17]+x[18]+x[19]+x[20]+x[21]+x[22]+x[23]+x[24]+x[25]+x[26]+x[27]+x[28]+x[29]+x[30]+x[31]+x[32]+x[33]+x[34]+x[35]+x[36]+x[37]+x[38]+x[39]+x[40]+x[41]+x[42]+x[43]+x[44]+x[45]+x[46]+x[47]+x[48]+x[49]))
results_df=rdd1.toDF(["meter","date","totalreading"])
##repartition to single
results_single_df = results_df.repartition(1)
results_dyf = DynamicFrame.fromDF(results_single_df, glueContext, "results_dyf")
applymapping1 = ApplyMapping.apply(frame = results_dyf, mappings = [("meter", "string", "meter", "string"), ("date", "string", "date", "string"), ("totalreading", "long", "totalreading", "long")], transformation_ctx = "applymapping1")
## @type: ApplyMapping
## @return: applymapping1
## @inputs: [frame = datasource0]

## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "getntrackconn", connection_options = {"dbtable": "gentrack_raw_zone", "database": "postgres"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "getntrackconn", connection_options = {"dbtable": "consumptiondb.gentrack_transform", "database": "postgres"}, transformation_ctx = "datasink4")
job.commit()