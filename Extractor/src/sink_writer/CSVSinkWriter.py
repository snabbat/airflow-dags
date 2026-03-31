from pyspark.sql import DataFrame
# ALTERED: SparkManager import commented out — Spark read/write not available locally (no Iceberg/S3A)
# from spark_manager import SparkManager
from context_manager.JsonAccessorManager import JsonAccessorManager
import boto3


class CSVSinkWriter:
    """
    Gere l'ecriture et la finalisation des fichiers CSV vers S3.
    """

    def __init__(self, accessor_manager: JsonAccessorManager):
        self.accessor_manager = accessor_manager
        self.config = accessor_manager.get("shared_config")
        self.shared_config = self.config.get_metadata()
        self.s3_config = self.shared_config['s3_config']
        # ALTERED: SparkManager commented out — Spark read/write not available locally (no Iceberg/S3A)
        # self.spark = SparkManager.get_instance()
        # ALTERED: boto3 S3 client commented out — endpoint S3 non accessible localement
        # self.s3 = boto3.client(
        #     's3',
        #     aws_access_key_id=self.s3_config['access_key'],
        #     aws_secret_access_key=self.s3_config['secret_access_key'],
        #     endpoint_url=self.s3_config['host_name'],
        #     verify=False
        # )

    def writeCSV(self, spark_df: DataFrame, delimiter: str, encoding: str, header: str, s3_path: str) -> bool:
        """
        Ecrit un DataFrame Spark en CSV vers un chemin S3 temporaire.
        Le coalesce(1) permet de produire un seul fichier de sortie.
        """
        try:
            spark_df.coalesce(1).write \
                .option('delimiter', delimiter) \
                .option('encoding', encoding) \
                .option("header", header) \
                .mode("overwrite") \
                .csv(s3_path)
            print(f"Donnees CSV ecrites dans {s3_path}")
            return True
        except Exception as e:
            print(f"Erreur lors de l'ecriture CSV vers S3 : {e}")
            return False

    def finalizeCSV(self, s3_path_tmp: str, s3_path: str, extension: str):
        """
        Finalise l'ecriture CSV en renommant le fichier temporaire produit par Spark
        vers le chemin final avec la bonne extension.
        Spark ecrit dans un dossier temporaire avec des noms comme part-00000-*.csv,
        cette methode copie le bon fichier vers le chemin final et nettoie le dossier tmp.
        """
        try:
            if s3_path_tmp.startswith("s3a://"):
                s3_path_tmp = s3_path_tmp.replace("s3a://", "")
            if s3_path.startswith("s3a://"):
                s3_path = s3_path.replace("s3a://", "")

            bucket_tmp, prefix_tmp = s3_path_tmp.split("/", 1)
            bucket_final, prefix_final = s3_path.split("/", 1)

            # Chercher le fichier .csv produit par Spark dans le dossier temporaire
            objects = self.s3.list_objects_v2(Bucket=bucket_tmp, Prefix=prefix_tmp)
            for obj in objects.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv"):
                    self.s3.copy_object(
                        Bucket=bucket_final,
                        CopySource={"Bucket": bucket_tmp, "Key": key},
                        Key=prefix_final + f".{extension}"
                    )
                    print(f"Fichier CSV finalise dans s3a://{bucket_final}/{prefix_final}.{extension}")
                    break

            # Nettoyage du dossier temporaire
            objects_to_delete = self.s3.list_objects_v2(Bucket=bucket_tmp, Prefix="tmp")
            if "Contents" in objects_to_delete:
                for obj in objects_to_delete["Contents"]:
                    self.s3.delete_object(Bucket=bucket_tmp, Key=obj["Key"])

        except Exception as e:
            print(f"Erreur lors de la finalisation du CSV : {e}")
