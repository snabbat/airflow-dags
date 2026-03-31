from exception_manager.BaseExceptionHandler import BaseExceptionHandler
import traceback
import os
import re
from datetime import datetime


class ExceptionHandler(BaseExceptionHandler):
    """Gestionnaire concret d'exceptions. Resout le type, extrait les details Spark et resume la trace."""

    def __init__(self, config, next_handler=None):
        super().__init__(next_handler)
        self.config = config

    def handle_exception(self, exception, context):
        """Traite l'exception et fusionne le contexte additionnel (classe, flow, target, etc.)."""
        error_info = self.get_error_info(exception)
        error_info = {**error_info, **context}
        return error_info

    def _resolve_exception_type(self, exception):
        """Resout le type reel de l'exception en deballant les Py4JJavaError si necessaire."""
        exception_type = type(exception).__name__

        # Py4JJavaError encapsule souvent une AnalysisException ou autre erreur Spark
        if exception_type == "Py4JJavaError":
            error_msg = str(exception)
            # Detection des sous-types Spark encapsules dans le Py4JJavaError
            spark_exceptions = ["AnalysisException", "IllegalArgumentException",
                                "StreamingQueryException", "SparkUpgradeException",
                                "QueryExecutionException"]
            for spark_type in spark_exceptions:
                if spark_type in error_msg:
                    # Utiliser le type Spark si present dans la config, sinon garder Py4JJavaError
                    if spark_type in self.config:
                        return spark_type
            return exception_type

        return exception_type

    def get_error_info(self, exception):
        """Recupere les informations de l'erreur a partir de la configuration JSON."""
        exception_type = self._resolve_exception_type(exception)
        error_details = self.config.get(exception_type, self.config.get("default_error"))

        # Resume de la trace (fichier, ligne, fonction du point d'origine)
        stack_trace = self._summarize_traceback(exception)

        # Details supplementaires pour les erreurs Spark SQL
        spark_detail = self._extract_spark_detail(exception)

        try:
            username = os.getlogin()
        except OSError:
            username = os.environ.get('USER')

        error_info = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "User": username,
            "id": error_details["id"],
            "category": error_details["category"],
            "message": error_details["message"],
            "stack_trace": stack_trace
        }

        if spark_detail:
            error_info["spark_detail"] = spark_detail

        return error_info

    def _extract_spark_detail(self, exception):
        """Extrait les details specifiques des erreurs Spark (table introuvable, error_class, SQLSTATE)."""
        error_msg = str(exception)
        detail = {}

        # Extraction du error_class Spark (ex: TABLE_OR_VIEW_NOT_FOUND)
        error_class_match = re.search(r'\[(\w+)\]', error_msg)
        if error_class_match:
            detail["error_class"] = error_class_match.group(1)

        # Extraction du nom de la table/vue (format `schema`.`table`)
        table_match = re.search(r'`([^`]+)`\.`([^`]+)`', error_msg)
        if table_match:
            detail["schema"] = table_match.group(1)
            detail["table"] = table_match.group(2)

        # Extraction du code SQLSTATE
        sqlstate_match = re.search(r'SQLSTATE:\s*(\w+)', error_msg)
        if sqlstate_match:
            detail["sqlstate"] = sqlstate_match.group(1)

        return detail if detail else None

    def _summarize_traceback(self, exception):
        """Resume la trace en une ligne concise : fichier, ligne, fonction et message d'erreur."""
        tb = traceback.extract_tb(exception.__traceback__)
        if not tb:
            return f"{type(exception).__name__}: {exception}"

        # Recherche du dernier frame dans le code du projet (pas les librairies externes)
        project_root = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
        origin = tb[-1]
        for frame in reversed(tb):
            if project_root in os.path.normpath(frame.filename):
                origin = frame
                break

        filename = os.path.basename(origin.filename)
        # Premier paragraphe du message d'erreur (avant les details Spark)
        error_msg = str(exception).split('\n')[0]
        return f"{type(exception).__name__} dans {filename}:{origin.lineno} ({origin.name}) — {error_msg}"
