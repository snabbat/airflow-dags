from pyspark.sql import SparkSession
import pandas as pd
import socket
import os
import json
import logging
from context_manager.JsonAccessorManager import JsonAccessorManager
import sys


driver_ip = socket.gethostbyname(socket.gethostname())

# Classe Singleton pour la gestion de la session Spark
class SparkManager:
    accessor_manager = JsonAccessorManager()
    path = os.path.normpath(os.path.join(sys.path[0], "..", "..", "..", "config"))
    accessor_manager.add_from_directory(path)
    _instance: SparkSession = None
    shared_config = accessor_manager.get('shared_config').get_metadata()
    s3_config = shared_config['s3_config']
    spark_config = shared_config['spark_config']
    jar_dir = spark_config['jar_dir']
    hms_config = shared_config['hms_config']

    @classmethod
    def get_instance(self) -> SparkSession:
        if self._instance is None:
            # Construction de la session Spark en mode local
            self._instance = SparkSession.builder \
                                .appName(self.spark_config['app_name']) \
                                .master("local[*]") \
                                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                                .config("spark.sql.analyzer.maxIterations", "1000") \
                                .config("spark.driver.memory", "2g") \
                                .getOrCreate()
            self._instance.sparkContext.setLogLevel("OFF")

            # Suppression des logs internes de Spark (SQLQueryContextLogger, Py4JJavaError, etc.)
            logging.getLogger("py4j").setLevel(logging.CRITICAL)
            logging.getLogger("pyspark").setLevel(logging.CRITICAL)
            logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)
            try:
                # Spark 3.x — log4j v1
                log4j = self._instance._jvm.org.apache.log4j
                log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j.Level.OFF)
                log4j.LogManager.getLogger("org.apache.spark.sql.catalyst").setLevel(log4j.Level.OFF)
                log4j.LogManager.getLogger("org.apache.spark.sql.execution").setLevel(log4j.Level.OFF)
            except Exception:
                pass
            try:
                # Spark 4.x — log4j2 via LogManager
                log_manager = self._instance._jvm.org.apache.logging.log4j.LogManager
                level_off = self._instance._jvm.org.apache.logging.log4j.Level.OFF
                log_manager.getLogger("org.apache.spark").setLevel(level_off)
                log_manager.getLogger("org.apache.spark.sql.catalyst").setLevel(level_off)
                log_manager.getLogger("org.apache.spark.sql.execution").setLevel(level_off)
            except Exception:
                pass

            for key, value in sorted(self._instance.sparkContext.getConf().getAll()):
                if "ansi" in key or "decimal" in key or "legacy" in key:
                    print(f'{key} = {value}')
        return self._instance


    @classmethod
    def stop_instance(self) -> bool:
        if self._instance is None:
            return False
        else:
            try:
                self._instance.stop()
                return True
            except Exception as e:
                return False
