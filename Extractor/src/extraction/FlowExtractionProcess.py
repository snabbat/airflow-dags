from datetime import datetime
import sys
import os

from context_manager.JsonAccessorManager import JsonAccessorManager
from exception_manager.ExceptionManager import ExceptionManager
from logger.LogHandler import LogHandler
from sink_writer.CSVSinkWriter import CSVSinkWriter
from sink_writer.XMLSinkWriter import XMLSinkWriter
from py4j.protocol import Py4JJavaError
from pyspark.errors.exceptions.captured import AnalysisException
import re
# ALTERED: SparkManager re-enabled pour executer les requetes SQL d'extraction via spark.sql()
from spark_manager import SparkManager


# ALTERED: renamed from FlowIngestionProcessCible, moved from ingestion/ to extraction/
class FlowExtractionProcess:
    # ALTERED: added target parameter to build correct metadata path (country/vertical/target/system/flow.json)
    def __init__(self, accessor_manager: JsonAccessorManager, country: str, vertical: str,
                 target: str, system_target: str, flow_name: str, exception_manager: ExceptionManager):
        """
        Initialise un processus d'extraction avec les paramètres requis.
        :param country: Pays concerné
        :param vertical: Verticale métier
        :param target: Cible de données (ex: conformite, finance)
        :param system_target: Système source des données
        :param flow_name: Nom du flow à traiter
        """
        # ALTERED: key now includes target level — was: country-vertical-system-flow
        self.key = country + "-" + vertical + "-" + target + "-" + system_target + "-" + flow_name
        self.target = target
        self.flow_name = flow_name
        self.country = country
        self.system_target = system_target
        self.vertical = vertical
        self.accessor_manager = accessor_manager
        self.exception_manager = exception_manager
        self.flow_accessor = accessor_manager.get(self.key)
        self.config = accessor_manager.get("shared_config")
        self.project_root = os.path.normpath(os.path.join(sys.path[0], "..", ".."))
        self.metadata_path = os.path.normpath(os.path.join(self.project_root, self.config.get_global_config_extractor_metadata_root(), self.key.replace("-", "/") + ".json"))
        # ALTERED: added sql_path — extraction query file co-located with JSON metadata
        self.sql_path = self.metadata_path.replace(".json", ".sql")
        self.execution_date = self.accessor_manager.get("shared_config").get("runtime_config")["execution_date"]

        self.logger = LogHandler(self.accessor_manager, "Extraction")

        self.init_reader_writer()
        # ALTERED: added target to dag_config key
        self.dag_config = country + "-" + vertical + "-" + target + "-" + system_target + "-common"
        self.system_target = system_target
        self.status = {'status': 'ok', 'reason': ''}

    def run(self) -> bool:
        try:
            start_time = datetime.now()
            # ALTERED: SparkManager.get_instance() commented out — Spark read/write not available locally (no Iceberg/S3A)
            # spark = SparkManager.get_instance()
            # ALTERED: log messages rewritten pour decrire chaque etape du traitement
            self.logger.log_start(0, f"Demarrage du traitement d'extraction du flow {self.flow_name} (target: {self.target}, systeme: {self.system_target}, date: {self.execution_date})")

            self.logger.log_process(1, f"Verification de l'existence du fichier metadata du flow {self.flow_name} dans {self.metadata_path}")
            if not os.path.isfile(self.metadata_path):
                self.logger.log_fail(-4, f"Le fichier metadata du flow {self.flow_name} est introuvable dans le chemin {self.metadata_path}")
                self.status = {'status': 'ko', 'reason': f"Fichier metadata introuvable : {self.metadata_path}"}
                return False
            self.logger.log_process(11, f"Le fichier metadata du flow {self.flow_name} a ete trouve avec succes")

            self.logger.log_process(2, f"Construction des chemins de sortie pour le flow {self.flow_name}")
            self.build_paths()
            self.logger.log_process(21, f"Les donnees du flow {self.flow_name} seront ecrites dans {self.output_data_file_path}")

            self.logger.log_process(3, f"Chargement de la requete SQL d'extraction du flow {self.flow_name} depuis {self.sql_path}")
            self.sql_query = self.load_sql()
            if self.sql_query is None:
                self.logger.log_fail(-5, f"Le fichier SQL du flow {self.flow_name} est introuvable dans le chemin {self.sql_path}")
                self.status = {'status': 'ko', 'reason': f"Fichier SQL introuvable : {self.sql_path}"}
                return False
            self.logger.log_process(31, f"La requete SQL du flow {self.flow_name} a ete chargee et les parametres (schemas, date) ont ete resolus")

            self.logger.log_process(4, f"Chargement de la configuration du flow {self.flow_name} a partir du metadata")

            self.logger.log_process(5, f"Les pre-controles sur le flow {self.flow_name} ne sont pas encore implementes, etape ignoree")

            # ALTERED: execution de la requete SQL via SparkManager pour obtenir le DataFrame
            self.logger.log_process(6, f"Execution de la requete SQL d'extraction du flow {self.flow_name} via Spark")
            spark = SparkManager.get_instance()
            self.extracted_data = spark.sql(self.sql_query)
            row_count = self.extracted_data.count()
            self.flow_accessor.set("initial_rows_number", row_count)
            self.logger.log_process(61, f"La requete SQL du flow {self.flow_name} a retourne {row_count} lignes")

            self.logger.log_process(7, f"Debut de l'ecriture des donnees du flow {self.flow_name}")
            output_config = self.flow_accessor.get_metadata().get('output', {})
            output_type = output_config.get('type', 'csv')
            output_bucket = os.path.normpath(os.path.join(self.project_root, self.config.get("global_config").get("data_output_root")))
            s3_path_tmp = os.path.join(output_bucket, "tmp")
            s3_path_final = os.path.join(output_bucket, self.execution_date, self.key.replace("-", os.sep))

            if output_type == 'csv':
                self.logger.log_process(92, f"Ecriture du flow {self.flow_name} au format CSV dans {s3_path_tmp}")
                write_success = self.csv_writer.writeCSV(
                    self.extracted_data,
                    output_config.get('delimiter', ';'),
                    output_config.get('encoding', 'UTF-8'),
                    str(output_config.get('header', True)),
                    s3_path_tmp
                )
                if not write_success:
                    self.logger.log_fail(-7, f"L'ecriture CSV du flow {self.flow_name} a echoue")
                    self.status = {'status': 'ko', 'reason': f"Echec de l'ecriture CSV du flow {self.flow_name}"}
                    return False
                extension = output_config.get('extension', 'csv')
                self.csv_writer.finalizeCSV(s3_path_tmp, s3_path_final, extension)
                self.logger.log_process(93, f"Fichier CSV du flow {self.flow_name} finalise dans {s3_path_final}.{extension}")
            elif output_type == 'xml':
                self.logger.log_process(92, f"Ecriture du flow {self.flow_name} au format XML dans {s3_path_tmp}")
                write_success = self.xml_writer.writeXML(self.extracted_data, output_config, s3_path_tmp)
                if not write_success:
                    self.logger.log_fail(-7, f"L'ecriture XML du flow {self.flow_name} a echoue")
                    self.status = {'status': 'ko', 'reason': f"Echec de l'ecriture XML du flow {self.flow_name}"}
                    return False
                self.xml_writer.finalizeXML(s3_path_tmp, s3_path_final)
                self.logger.log_process(93, f"Fichier XML du flow {self.flow_name} finalise dans {s3_path_final}.xml")
            else:
                self.logger.log_fail(-6, f"Format de sortie inconnu '{output_type}' pour le flow {self.flow_name}")
                self.status = {'status': 'ko', 'reason': f"Format de sortie inconnu : {output_type}"}
                return False

            self.flow_accessor.set("written_rows", row_count)
            self.logger.log_process(94, f"Ecriture du flow {self.flow_name} terminee avec succes ({row_count} lignes)")

            self.logger.log_process(10, f"Les post-controles sur le flow {self.flow_name} ne sont pas encore implementes, etape ignoree")

            self.logger.log_complete(9999, f"Fin du traitement d'extraction du flow {self.flow_name} avec succes (target: {self.target}, systeme: {self.system_target})")
            return True

        except AnalysisException as e:
            # Spark SQL AnalysisException — table/vue introuvable, syntaxe SQL incorrecte, etc.
            error_msg = str(e)
            table_match = re.search(r'`([^`]+)`\.`([^`]+)`', error_msg)
            if table_match:
                schema_name, table_name = table_match.group(1), table_match.group(2)
                detail = f"La table ou vue `{schema_name}`.`{table_name}` est introuvable"
            else:
                detail = "Erreur d'analyse SQL Spark"
            error_class_match = re.search(r'\[(\w+)\]', error_msg)
            error_class = error_class_match.group(1) if error_class_match else "UNKNOWN"
            self.status = {'status': 'ko', 'reason': f"{detail} lors du traitement du flow {self.flow_name}"}
            self.exception_manager.handle(e, {
                "classe": __name__, "flow": self.flow_name, "target": self.target,
                "system": self.system_target, "error_class": error_class,
                "detail": detail
            })
            self.logger.log_fail(-2, f"{detail} — flow {self.flow_name}, requete SQL {self.sql_path} (error_class: {error_class})")
            return False

        except Py4JJavaError as e:
            # Py4JJavaError peut encapsuler une AnalysisException ou une erreur memoire/JVM
            error_msg = str(e)
            if "AnalysisException" in error_msg:
                table_match = re.search(r'`([^`]+)`\.`([^`]+)`', error_msg)
                if table_match:
                    schema_name, table_name = table_match.group(1), table_match.group(2)
                    detail = f"La table ou vue `{schema_name}`.`{table_name}` est introuvable"
                else:
                    detail = "Erreur d'analyse SQL Spark"
                error_class_match = re.search(r'\[(\w+)\]', error_msg)
                error_class = error_class_match.group(1) if error_class_match else "UNKNOWN"
                self.status = {'status': 'ko', 'reason': f"{detail} lors du traitement du flow {self.flow_name}"}
                self.exception_manager.handle(e, {
                    "classe": __name__, "flow": self.flow_name, "target": self.target,
                    "system": self.system_target, "error_class": error_class,
                    "detail": detail
                })
                self.logger.log_fail(-2, f"{detail} — flow {self.flow_name}, requete SQL {self.sql_path} (error_class: {error_class})")
            else:
                self.status = {'status': 'ko', 'reason': f"Erreur Spark/JVM lors du traitement du flow {self.flow_name}"}
                self.exception_manager.handle(e, {"classe": __name__, "flow": self.flow_name, "target": self.target, "system": self.system_target})
                self.logger.log_fail(-2, f"Le traitement du flow {self.flow_name} a echoue a cause d'une erreur Spark/JVM (verifier spark.executor.memory et spark.driver.memory)")
            return False

        except Exception as e:
            self.status = {'status': 'ko', 'reason': f"Erreur inattendue lors du traitement du flow {self.flow_name} : {str(e)}"}
            self.exception_manager.handle(e, {"classe": __name__, "flow": self.flow_name, "target": self.target, "system": self.system_target})
            self.logger.log_fail(-3, f"Le traitement du flow {self.flow_name} a echoue avec une erreur inattendue : {str(e)}")
            return False

        finally:
            # Logging execution stats
            try:
                initial_row_number = self.accessor_manager.get(self.key).get("initial_rows_number")
                written_rows = self.accessor_manager.get(self.key).get("written_rows")
                if initial_row_number is None:
                    initial_row_number = 0
                if written_rows is None:
                    written_rows = 0
            except Exception:
                initial_row_number = 0
                written_rows = 0

            # ALTERED: df_log commented out — its Iceberg write below is disabled, so it's unused
            # df_log = self.logger.get_log_dataframe(
            #     execution_date=self.execution_date,
            #     country=self.country,
            #     vertical=self.vertical,
            #     system_target=self.system_target,
            #     flow_name=self.flow_name
            # )
            # ALTERED: Iceberg log write commented out — Spark read/write not available locally (no Iceberg/S3A)
            # df_log.write.format("iceberg").option("path", "s3a://monitoring/Logger").mode("append").saveAsTable("monitoring.logger", partitionBy="execution_date")
            self.logger.reset_steps()

            # ALTERED: improved summary log with full context
            end_time = datetime.now()
            time_diff = end_time - start_time
            total_seconds = int(time_diff.total_seconds())
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted_time_diff = f"{hours:02}:{minutes:02}:{seconds:02}"
            print("---------------------------------------------------------------------------------------------------------")
            print(f"--> Fin du traitement d'extraction du flow {self.flow_name} avec le statut : {self.status['status']}")
            if self.status.get('reason'):
                print(f"--> Raison : {self.status['reason']}")
            print(f"--> Lignes lues : {initial_row_number} | Lignes ecrites : {written_rows} | Lignes rejetees : {initial_row_number - written_rows}")
            print(f"--> Temps d'execution : {formatted_time_diff}")
            print("---------------------------------------------------------------------------------------------------------")

            # ALTERED: Spark stats collection and Iceberg write commented out — Spark read/write not available locally (no Iceberg/S3A)
            # cpu = spark.sparkContext.getConf().get("spark.executor.cores")
            # memory = spark.sparkContext.getConf().get("spark.executor.memory")
            # data = [Row(
            #     frequency='Q',
            #     execution_date=self.execution_date,
            #     country=self.country,
            #     vertical=self.vertical,
            #     system_target=self.system_target,
            #     flow_name=self.flow_name,
            #     start_time=start_time,
            #     end_time=end_time,
            #     executime_lapse=total_seconds,
            #     execution_time=formatted_time_diff,
            #     cpu=cpu,
            #     memory=memory,
            #     initial_row_count=initial_row_number,
            #     extracted_row_number=written_rows,
            #     rejected_row_number=initial_row_number - written_rows,
            #     status=self.status['status'],
            #     reason=self.status['reason']
            # )]
            # df_stats = spark.createDataFrame(data)
            # df_stats.write.format("iceberg").option("path", "s3a://monitoring/execution_stats").mode("append").saveAsTable("monitoring.execution_stats", partitionBy="execution_date")

    # ALTERED: added load_sql() to read the extraction SQL query and resolve placeholders
    def load_sql(self):
        """Charge la requete SQL d'extraction et resout les placeholders."""
        if not os.path.isfile(self.sql_path):
            return None
        with open(self.sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        # Resolve placeholders from flow metadata
        metadata = self.flow_accessor.get_metadata()
        db_schemas = metadata.get("db_schemas", [])
        for i, schema in enumerate(db_schemas):
            sql = sql.replace("{$Db_Schema" + str(i + 1) + "}", schema)
        sql = sql.replace("{$ID_DATE}", "'" + self.execution_date + "'")
        return sql

    def build_paths(self):
        try:
            key_to_path = self.key.replace("-", os.sep)
            output_root = os.path.normpath(os.path.join(self.project_root, self.config.get("global_config").get("data_output_root")))
            self.output_data_file_path = os.path.join(output_root, self.execution_date, key_to_path + ".parquet")
            self.reject_transformed_data_file_path = os.path.join(output_root, self.execution_date, key_to_path + "_trans_rjt.parquet")
        except Exception as e:
            self.exception_manager.handle(e, {"classe": __name__, "flow": self.flow_name})
            self.logger.log_fail(-2, "METADATA INDISPO")
            return False

    def init_reader_writer(self):
        self.csv_writer = CSVSinkWriter(self.accessor_manager)
        self.xml_writer = XMLSinkWriter(self.accessor_manager)
