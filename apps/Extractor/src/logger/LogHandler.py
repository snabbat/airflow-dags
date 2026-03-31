from context_manager.JsonAccessorManager import JsonAccessorManager
from logger.DataProcess import DataProcess
from sink_writer.log_writer.LogPrintWriter import LogPrintWriter
from sink_writer.log_writer.LogDataFrameWriter import LogDataFrameWriter


class LogHandler:
    """Gestionnaire de logs. Orchestre l'enregistrement des evenements de traitement via les writers."""

    def __init__(self, accessor_manager: JsonAccessorManager, phase):
        self.accessor_manager = accessor_manager
        self.data_process = DataProcess(self.accessor_manager, phase)
        self.sink_writer = LogPrintWriter()
        self.sink_writer_dataframe = LogDataFrameWriter()

    def log_start(self, code, message="Processing started"):
        """Enregistre le demarrage du traitement."""
        self.data_process.start(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_process(self, code, message="Processing ongoing"):
        """Enregistre un traitement en cours."""
        self.data_process.process(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_complete(self, code, message="Process completed successfully"):
        """Enregistre la fin reussie du traitement."""
        self.data_process.complete(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_fail(self, code, message="Process failed"):
        """Enregistre l'echec du traitement."""
        self.data_process.fail(code, message)
        self.sink_writer.log_data_process(self.data_process.to_dict())
        self.sink_writer_dataframe.log_data_process(self.data_process.to_dict())

    def log_step(self, message):
        """Enregistre une etape intermediaire du traitement."""
        self.data_process.add_step(message)
        step = self.data_process.get_last_step().to_dict()
        step["phase"] = self.data_process.phase
        self.sink_writer.log_data_process_step(step)
        self.sink_writer_dataframe.log_data_process_step(step)

    def get_log_dataframe(self, execution_date: str, country: str, vertical: str, system_target: str, flow_name: str):
        """Retourne les logs sous forme de liste de dictionnaires."""
        return self.sink_writer_dataframe.get_log_dataframe()

    def reset_steps(self):
        """Reinitialise les etapes enregistrees."""
        self.sink_writer_dataframe.reset_steps()
