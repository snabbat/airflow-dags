import os
from sink_writer.exception_writer.ExceptionWriter import ExceptionWriter


class ExceptionFileWriter(ExceptionWriter):
    """
    Ecrit les exceptions formatees dans un fichier log.
    Cree le repertoire parent si necessaire.
    """

    def __init__(self, log_path: str):
        self.log_path = log_path
        # Creer le repertoire parent s'il n'existe pas
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def format_exception(self, error_info: dict) -> str:
        lines = [
            "==================== EXCEPTION ====================",
            f"  Date      : {error_info.get('date', 'N/A')}",
            f"  User      : {error_info.get('User', 'N/A')}",
            f"  ID        : {error_info.get('id', 'N/A')}",
            f"  Categorie : {error_info.get('category', 'N/A')}",
            f"  Message   : {error_info.get('message', 'N/A')}",
            f"  Classe    : {error_info.get('classe', 'N/A')}",
            f"  Flow      : {error_info.get('flow', 'N/A')}",
            f"  Target    : {error_info.get('target', 'N/A')}",
            f"  System    : {error_info.get('system', 'N/A')}",
        ]
        # Details Spark specifiques (table introuvable, error_class, SQLSTATE)
        spark_detail = error_info.get('spark_detail')
        if spark_detail:
            lines.append(f"  Spark     :")
            if 'error_class' in spark_detail:
                lines.append(f"    Error Class : {spark_detail['error_class']}")
            if 'schema' in spark_detail and 'table' in spark_detail:
                lines.append(f"    Table       : {spark_detail['schema']}.{spark_detail['table']}")
            if 'sqlstate' in spark_detail:
                lines.append(f"    SQLSTATE    : {spark_detail['sqlstate']}")
        if error_info.get('detail'):
            lines.append(f"  Detail    : {error_info.get('detail')}")
        lines.extend([
            f"  Trace     :",
            f"  {error_info.get('stack_trace', 'N/A')}",
            "==================================================="
        ])
        return "\n".join(lines)

    def log_exception(self, formatted_exception: str):
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(formatted_exception + "\n")
