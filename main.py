from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from datetime import datetime, timedelta

from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def plustwo(n):
    out = n + 2
    return out


def falldist(t,g=9.81):
    d = 0.5 * g * t**2
    return d

def wrapperFunction(JobId):
    # username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    # sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    # Querydata = 'Select * From Job_Rule_Assignment Where Job_Id = ' + str(JobId)
    # JobRuleDetails_df = spark.read.format("jdbc") \
    #     .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
    #     .option("query", Querydata) \
    #     .option("user", username) \
    #     .option("password", sqlpassword) \
    #     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #     .load()
    # # Rule_df = Rule_df.filter(Rule_df.Rule_Name == RuleName)
    # JobRuleDetails_pdf = JobRuleDetails_df.toPandas()
    # # print(JobRuleDetails_pdf)
    # for ind in JobRuleDetails_pdf.index:
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 1):
    #         CheckJobRule_CountValidation(1, 1, 'abfss://targetcontainer@adlstoadls.dfs.core.windows.net/Log_directory/',
    #                                      'Bluesky.txt',
    #                                      'CountValidation')
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 2):
    #         print("2")
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 3):
    #         CheckJobRule_ThresoldValidation(1, 1,
    #                                         'abfss://targetcontainer@adlstoadls.dfs.core.windows.net/Log_directory/',
    #                                         'Bluesky.txt',
    #                                         'ThresholdValidation')
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 4):
    #         CheckJobRule_FileNameValidation(1, 1,
    #                                         'abfss://targetcontainer@adlstoadls.dfs.core.windows.net/Log_directory/',
    #                                         'Bluesky.txt',
    #                                         'FileNameValidation')
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 5):
    #         CheckJobRule_FileSizeValidation(1, 1,
    #                                         'abfss://targetcontainer@adlstoadls.dfs.core.windows.net/Log_directory/',
    #                                         'Bluesky.txt',
    #                                         'FileSizeValidation')
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 6):
    #         print("6")
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 7):
    #         print("7")
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 8):
    #         print("8")
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 9):
    #         print("9")
    #     if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 10):
    #         print("10")
    return JobId
    # print(JobRuleDetails_pdf)

def CheckJobRule_CountValidation(JobId, FileId, FilePath, FileName, RuleName):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(1)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == FileName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    Src_Count = int(AuditTableFileCount_pdf['File_Count'])

    spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                   dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                   )
    RawLogData_df = spark.read.text(FilePath + FileName)
    Tgt_Count = int(RawLogData_df.count())
    if (Src_Count == Tgt_Count):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": Src_Count,
                                    "Target_Value": Tgt_Count,
                                    "Source_Value_Type": 'Source Count',
                                    "Target_Value_Type": 'Target Count',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": 'hegautam@deloitte.com'}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "dbo.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()


def CheckJobRule_FileNameValidation(JobId, FileId, FilePath, FileName, RuleName):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(4)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTable_df = int(AuditTable_df.filter(AuditTable_df.File_Name == FileName).count())
    if (AuditTable_df > 0):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'

    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": FileName,
                                    "Target_Value": FileName,
                                    "Source_Value_Type": 'File Name',
                                    "Target_Value_Type": 'File Name',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": 'hegautam@deloitte.com'}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))

    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "dbo.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()


def CheckJobRule_FileSizeValidation(JobId, FileId, FilePath, FileName, RuleName):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(5)

    AuditTable_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from Audit_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == FileName)
    AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()
    SourceValue = AuditTableFileCount_pdf.loc[0]['File_Size']
    print(SourceValue)

    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from File_Metadata") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == FileName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    TargetValue = FileMetadata_pdf.loc[0]['FileSize']

    if (SourceValue == TargetValue):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Source File Size',
                                    "Target_Value_Type": 'Target File Size',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": 'hegautam@deloitte.com'}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "dbo.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()


def CheckJobRule_ThresoldValidation(JobId, FileId, FilePath, FileName, RuleName):
    username = dbutils.secrets.get(scope="dbscope1", key="sqluser")
    sqlpassword = dbutils.secrets.get(scope="dbscope1", key="sqlpass")
    RuleId = int(3)

    FileMetadata_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("query", "Select * from File_Metadata") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == FileName)
    FileMetadata_pdf = FileMetadata_df.toPandas()
    SourceValue_min = FileMetadata_pdf.loc[0]['MinValue']
    SourceValue_max = FileMetadata_pdf.loc[0]['MaxValue']
    SourceValue = 'MinValue = ' + str(SourceValue_min) + ' and MaxValue = ' + str(SourceValue_max)

    spark.conf.set(f"fs.azure.account.key.adlstoadls.dfs.core.windows.net",
                   dbutils.secrets.get("adls2adlsscope", "storageaccountkey")
                   )
    RawLogData_df = spark.read.text(FilePath + FileName)
    TargetValue = int(RawLogData_df.count())

    if (TargetValue >= SourceValue_min and TargetValue <= SourceValue_max):
        RuleStatus = 'Passed'
    else:
        RuleStatus = 'Failed'
    Created_Time = datetime.now()

    Job_Rule_Execution_Log_list = [{"Job_Id": JobId,
                                    "Rule_Id": RuleId,
                                    "Source_Value": SourceValue,
                                    "Target_Value": TargetValue,
                                    "Source_Value_Type": 'Threshold Value',
                                    "Target_Value_Type": 'Target File Count',
                                    "Source_Name": 'AuditTable',
                                    "Target_Name": 'ADLS',
                                    "Rule_Run_Status": RuleStatus,
                                    "Created_Time": Created_Time,
                                    "Created_By": 'hegautam@deloitte.com'}
                                   ]
    Job_Rule_Execution_Log_df = spark.createDataFrame(Job_Rule_Execution_Log_list)
    Job_Rule_Execution_Log_df = Job_Rule_Execution_Log_df.withColumn("Created_Time", to_timestamp("Created_Time"))
    Job_Rule_Execution_Log_df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://testsql-ss.database.windows.net:1433;database=testsqldb-ss") \
        .option("dbtable", "dbo.Job_Rule_Execution_Log") \
        .option("user", username) \
        .option("password", sqlpassword) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()







