from typing import Dict

from sink_writer.log_writer.LogSinkWriter import LogSinkWriter


class LogPrintWriter(LogSinkWriter):
    """Affiche les logs sur la sortie standard (console)."""

    def log_data_process(self, data_process: Dict):
        """Affiche les informations du processus principal."""
        formatted_data = " | ".join(f"{key}: {value}" for key, value in data_process.items())
        print(f"Log DataProcess: {formatted_data}")

    def log_data_process_step(self, data_process_step: Dict):
        """Affiche les informations d'une etape specifique."""
        formatted_step = " | ".join(f"{key}: {value}" for key, value in data_process_step.items())
        print(f"Log DataProcessStep: {formatted_step}")
