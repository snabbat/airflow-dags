from typing import Dict

from sink_writer.log_writer.LogSinkWriter import LogSinkWriter


class LogPrintWriter(LogSinkWriter):
    """
    Implémentation concrète de LogSinkWriter qui affiche les logs avec print.
    """

    def log_data_process(self, data_process: Dict):
        formatted_data = " | ".join(f"{key}: {value}" for key, value in data_process.items())
        print(f"Log DataProcess: {formatted_data}")

    def log_data_process_step(self, data_process_step: Dict):
        formatted_step = " | ".join(f"{key}: {value}" for key, value in data_process_step.items())
        print(f"Log DataProcessStep: {formatted_step}")



