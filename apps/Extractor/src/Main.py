from context_manager.JsonAccessorManager import JsonAccessorManager
from dag_manager.DAGLoader import DAGLoader
from exception_manager.ExceptionManager import ExceptionManager
from sink_writer.exception_writer.ExceptionFileWriter import ExceptionFileWriter
from sink_writer.exception_writer.ExceptionPrintWriter import ExceptionPrintWriter
from datetime import datetime
import sys
from logger.LogHandler import LogHandler
from spark_manager import SparkManager
import os

# Racine du projet — apps/Extractor/src/ → ../../.. → racine du projet
PROJECT_ROOT = os.path.normpath(os.path.join(sys.path[0], "..", "..", ".."))

if __name__ == '__main__':

    try:

        accessor_manager = JsonAccessorManager()
        accessor_manager.add_from_directory(os.path.join(PROJECT_ROOT, "metadata", "extractor"))
        accessor_manager.add_from_directory(os.path.join(PROJECT_ROOT, "config"))

        # Initialisation du gestionnaire d'exceptions avec les writers fichier et console
        exceptions_config_path = os.path.join(PROJECT_ROOT, "config", "exceptions_config.json")
        exception_log_path = os.path.join(PROJECT_ROOT, "apps", "Extractor", "Log", "Exception.log")
        file_logger = ExceptionFileWriter(exception_log_path)
        print_logger = ExceptionPrintWriter()
        exception_manager = ExceptionManager(exceptions_config_path, file_logger, print_logger)

        # Initialisation de la session Spark avant le lancement des flows
        spark = SparkManager.get_instance()
        print(f"Session Spark initialisee : {spark.sparkContext.appName} (master: {spark.sparkContext.master})")

        # Lancement du traitement d'extraction
        start_time = datetime.now()
        dataloader = DAGLoader(accessor_manager=accessor_manager, country="Maroc", vertical="SAHAM_BANK", exception_manager=exception_manager)
        success = dataloader.execute(country="Maroc", phase="extraction", vertical="SAHAM_BANK")
        end_time = datetime.now()
        time_diff = end_time - start_time
        print("-----------------------------------------------------")
        print("Duree d'execution de la phase d'extraction : ", time_diff.total_seconds(), "secondes")
        print("-----------------------------------------------------")
    except Exception as e:
        print(str(e))

    finally:
        SparkManager.stop_instance()
