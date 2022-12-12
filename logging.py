from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from datetime import datetime, timedelta

from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
logs_info = []

def Errorlog(current_time, NOTEBOOK_PATH, error_message, log, error_type, ErrorID, ObjectRunID, ObjectName,
              ExecutionRunID):
    log = f"{current_time} info {NOTEBOOK_PATH} Error: {error_message}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Error type: {error_type}"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    data = [{"ErrorID": f'{ErrorID}',
             "ObjectRunID": f'{ObjectRunID}',
             "ObjectName": f'{ObjectName}',
             "ExecutionRunID": f'{ExecutionRunID}',
             "ErrorType": f'{error_type}',
             "ErrorCode": 404,
             "ErrorMessage": f'{error_message}'}]
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Schema to create the Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Creating Job_Error_Log Dataframe"
    logs_info.append(log)
    tempdf1 = spark.createDataFrame(data)
    log = f"{current_time} info {NOTEBOOK_PATH} Created Job_Error_Log Dataframe"
    logs_info.append(log)
    log = f"{current_time} info {NOTEBOOK_PATH} Saving Job_Error_Log Dataframe to a Table"
    logs_info.append(log)
    tempdf1.write.mode("append").saveAsTable("table_error")
    log = f"{current_time} info {NOTEBOOK_PATH} Saved Job_Error_Log Dataframe to a Table"
    logs_info.append(log)

