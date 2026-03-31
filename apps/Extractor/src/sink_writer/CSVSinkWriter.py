from pyspark.sql import DataFrame
from context_manager.JsonAccessorManager import JsonAccessorManager
import boto3


class CSVSinkWriter:
    """Gere l'ecriture et la finalisation des fichiers CSV vers S3."""

    def __init__(self, accessor_manager: JsonAccessorManager):
        self.accessor_manager = accessor_manager
        self.config = accessor_manager.get("shared_config")
        self.shared_config = self.config.get_metadata()
        self.s3_config = self.shared_config['s3_config']

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
        """
        try:
            if s3_path_tmp.startswith("s3a://"):
                s3_path_tmp = s3_path_tmp.replace("s3a://", "")
            if s3_path.startswith("s3a://"):
                s3_path = s3_path.replace("s3a://", "")

            bucket_tmp, prefix_tmp = s3_path_tmp.split("/", 1)
            bucket_final, prefix_final = s3_path.split("/", 1)

            # Recherche du fichier .csv produit par Spark dans le dossier temporaire
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
