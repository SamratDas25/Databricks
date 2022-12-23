from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


def File_Run_Stats_Insert(FileRunId, ExecutionRunID, JobRunID, FileId, LoadStartTime, SourceCount, TargetCount, LoadEndTime,
              DurationInSeconds,Status,ServerName,DatabaseName,TableNname,jdbcUsername,jdbcPassword):
    
    data = [{"FileRunId": f'{FileRunId}',
             "ExecutionRunID": f'{ExecutionRunID}',
             "JobRunID": f'{JobRunID}',
             "FileId": f'{FileId}',
             "LoadStartTime": f'{LoadStartTime}',
             "SourceCount": f'{SourceCount}',
             "TargetCount": f'{TargetCount}',
             "LoadEndTime": f'{LoadEndTime}',
             "DurationInSeconds": f'{DurationInSeconds}',
             "Status": f'{Status}',}]
    
    tempdf1 = spark.createDataFrame(data)
    spark.sql('TRUNCATE table File_Run_Stats_Table')
    tempdf1.write.mode("append").saveAsTable("File_Run_Stats_Table")
    
    table_to_dataframe = spark.sql('select * from File_Run_Stats_Table')
    
    table_to_dataframe.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{ServerName}.database.windows.net:1433;database={DatabaseName}") \
              .option("dbtable", f"{TableNname}")\
              .option("user", jdbcUsername)\
              .option("password",jdbcPassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    

def Batch_Run_Stats_Insert(ExecutionRunID, BatchID, SourceID, BatchStartTime, BatchEndTime, Status,ServerName,DatabaseName,TableNname,jdbcUsername,jdbcPassword):
    
    data = [{"ExecutionRunID": f'{ExecutionRunID}',
             "BatchID": f'{BatchID}',
             "SourceID": f'{SourceID}',
             "BatchStartTime": f'{BatchStartTime}',
             "BatchEndTime": f'{BatchEndTime}',
             "Status": f'{Status}',}]
    
    tempdf1 = spark.createDataFrame(data)
    spark.sql('TRUNCATE table Batch_Run_Stats_Table')
    tempdf1.write.mode("append").saveAsTable("Batch_Run_Stats_Table")
    
    table_to_dataframe = spark.sql('select * from Batch_Run_Stats_Table')
    
    table_to_dataframe.write \
              .mode("append")\
              .format("jdbc")\
              .option("url", f"jdbc:sqlserver://{ServerName}.database.windows.net:1433;database={DatabaseName}") \
              .option("dbtable", f"{TableNname}")\
              .option("user", jdbcUsername)\
              .option("password",jdbcPassword )\
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
              .save()
    
    
jdbcUsername = "testmiuser1"
jdbcPassword = "testmisqlpoc1@123"
ServerName = "testsql-ss"
DatabaseName = "testsqldb-ss"

File_Run_Stats_Insert(205, 1, 105, 1, '2022-12-14 13:22:15.707', 49, 49, '2022-12-14 13:22:33.880',
              10,'Completed',ServerName,DatabaseName,TableNname,jdbcUsername,jdbcPassword)
Batch_Run_Stats_Insert(1, 1, 1, '2022-12-14 01:00:00.000', '2022-11-22 02:00:00.000', 'Succeeded',ServerName,DatabaseName,TableNname,jdbcUsername,jdbcPassword)
