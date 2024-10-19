import sys
from utils_aws.integration import glue ,sts
from utils_aws.configspark import SparkSessionBuilder
from utils_aws.read import DataReader, DatabaseReader
from utils_aws.write import DataWriter
from utils_aws.logger import LoggerManager
from utils_aws import transform
from datetime import datetime, timedelta
import boto3

################################################
#               PARAMS
################################################
account_id    = sts().get_account_id()
glue_args     = glue().getArguments(sys.argv) #param
appname       = glue_args[0]
environment   = glue_args[1]
process_date  = glue_args[2]
full_load     = glue_args[3]
process_type  = glue_args[4]

################################################
#               CONFIG CLUSTER
################################################
spark_builder = SparkSessionBuilder(appname=appname) #param
spark_session = spark_builder.get_sparksession()
glue_context  = spark_builder.get_gluecontext()
glue_logger   = glue_context.get_logger()
reader        = DataReader(spark_session)
dbreader      = DatabaseReader(spark_session)
writer        = DataWriter()

################################################
#              DATES
################################################
today           = transform.DateGetter.current_datetime_in_timezone('America/Lima')
date_components = transform.DateGetter.extract_date_components(today)
year            = date_components['year']
month           = date_components['month']
day             = date_components['day']
end_date        = transform.DateManipulator.subtract_date(today,"days",1)
start_date      = transform.DateManipulator.subtract_date(today,"days",31)
end_date_str    = end_date.strftime('%Y-%m-%d')
start_date_str  = start_date.strftime('%Y-%m-%d')

if process_type    == "reprocess" :
    process_date_list = process_date.split(',')
    start_date_str = process_date_list[0]
    end_date_str   = process_date_list[1]
    
print(f"Fecha inicial: {start_date_str}")
print(f"Fecha final: {end_date_str}")
################################################
#                VARIABLES
################################################

if __name__ == "__main__":
    glue_logger.info("start pipeline.")
    glue_logger.info("end pipeline")