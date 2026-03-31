import os
from datetime import datetime, time

from context_manager.JsonAccessorManager import JsonAccessorManager
from logger.DataProcess import DataProcess
from sink_writer.log_writer.LogPrintWriter import LogPrintWriter
from sink_writer.log_writer.LogDataFrameWriter import LogDataFrameWriter
# ALTERED: pyspark imports commented out — Spark read/write not available locally (no Iceberg/S3A)
# import pandas as pd
# from pyspark.sql.functions import lit
# from pyspark.sql import DataFrame as SparkDataFrame

class LogHandler:

    def __init__(self, accessor_manager: JsonAccessorManager, phase):
        self.accessor_manager = accessor_manager
        self.data_process = DataProcess(self.accessor_manager, phase)
        self.sink_writer = LogPrintWriter()
        self.sink_writer_dataframe = LogDataFrameWriter()

    def log_start(self, code, message="Processing started"):
        self.data_process.start(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())
    
    def log_process(self, code, message="Processing ongoing"):
        self.data_process.process(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_complete(self, code, message="Process completed successfully"):
        self.data_process.complete(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_fail(self, code, message="Process failed"):
        self.data_process.fail(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_step(self, message):
        self.data_process.add_step(message)
        step = self.data_process.get_last_step().to_dict()
        step["phase"] = self.data_process.phase
        self.sink_writer.log_data_process_step(step)
        self.sink_writer_dataframe.log_data_process_step(step)

    # ALTERED: Spark DataFrame enrichment commented out — Spark read/write not available locally (no Iceberg/S3A)
    # def get_log_dataframe(self, execution_date: str, country: str, vertical: str, system_target: str, flow_name: str) -> SparkDataFrame:
    #     df_log: SparkDataFrame = self.sink_writer_dataframe.get_log_dataframe()
    #     df_log = df_log.withColumn("execution_date", lit(execution_date))
    #     df_log = df_log.withColumn("flow_name", lit(flow_name))
    #     df_log = df_log.withColumn("country", lit(country))
    #     df_log = df_log.withColumn("system_target", lit(system_target))
    #     df_log = df_log.withColumn("vertical", lit(vertical))
    #     return df_log
    def get_log_dataframe(self, execution_date: str, country: str, vertical: str, system_target: str, flow_name: str):
        return self.sink_writer_dataframe.get_log_dataframe()

    def reset_steps(self):
        self.sink_writer_dataframe.reset_steps()