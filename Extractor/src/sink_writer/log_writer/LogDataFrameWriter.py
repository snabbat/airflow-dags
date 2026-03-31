import json
from typing import List, Dict
from datetime import datetime
# ALTERED: SparkManager import commented out — Spark read/write not available locally (no Iceberg/S3A)
# from spark_manager import SparkManager
# from pyspark.sql.types import StructType, StructField, StringType

class LogDataFrameWriter:
    def __init__(self):
        self.logs: List[Dict] = []
        # ALTERED: Spark instance commented out — not available locally
        # self.spark = SparkManager.get_instance()

    def _timestamp(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _normalize_message(self, message):
        if isinstance(message, dict):
            return json.dumps(message, ensure_ascii=False)
        return str(message)

    def log_data_process(self, data_process: Dict):
        enriched = {
            "log_type": "process",
            "phase": data_process.get("phase"),
            "code": data_process.get("code"),
            "message": self._normalize_message(data_process.get("message")),
            "status": data_process.get("status"),
            "timestamp": self._timestamp(),
            "pid": data_process.get("pid"),
            "ppid": data_process.get("ppid"),
            "user": data_process.get("user"),
            "start_date": data_process.get("start_date"),
            "end_date": data_process.get("end_date"),
            "id_date": data_process.get("id_date"),
            "step_id": None,
            "step_datetime": None
        }
        self.logs.append(enriched)

    def log_data_process_step(self, data_process_step: Dict):
        enriched = {
            "log_type": "step",
            "phase": data_process_step.get("phase"),
            "code": None,
            "message": self._normalize_message(data_process_step.get("message")),
            "status": "step_recorded",
            "timestamp": self._timestamp(),
            "pid": data_process_step.get("id_process"),
            "ppid": None,
            "user": None,
            "start_date": None,
            "end_date": None,
            "id_date": None,
            "step_id": data_process_step.get("id"),
            "step_datetime": data_process_step.get("datetime")
        }
        self.logs.append(enriched)

    def get_log_dataframe(self):
        # ALTERED: Spark DataFrame creation commented out — Spark read/write not available locally (no Iceberg/S3A)
        # df_schema = StructType([
        #     StructField("log_type", StringType(), True),
        #     StructField("phase", StringType(), True),
        #     StructField("code", StringType(), True),
        #     StructField("message", StringType(), True),
        #     StructField("status", StringType(), True),
        #     StructField("timestamp", StringType(), True),
        #     StructField("pid", StringType(), True),
        #     StructField("ppid", StringType(), True),
        #     StructField("user", StringType(), True),
        #     StructField("start_date", StringType(), True),
        #     StructField("end_date", StringType(), True),
        #     StructField("id_date", StringType(), True),
        #     StructField("step_id", StringType(), True),
        #     StructField("step_datetime", StringType(), True)
        # ])
        # return self.spark.createDataFrame(self.logs, schema=df_schema)
        return self.logs

    def reset_steps(self):
        """Réinitialise la liste des étapes du processus"""
        self.logs = []
