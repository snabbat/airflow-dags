from sink_writer.exception_writer.ExceptionWriter import ExceptionWriter


class ExceptionPrintWriter(ExceptionWriter):
    """
    Affiche les exceptions formatees sur la sortie standard (console).
    """

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
        print(formatted_exception)
