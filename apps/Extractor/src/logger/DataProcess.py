import os
from datetime import datetime

from context_manager.JsonAccessorManager import JsonAccessorManager
from logger.DataProcessStep import DataProcessStep


class DataProcess:
    """Represente un processus de traitement avec son cycle de vie (demarrage, en cours, termine, echoue)."""

    def __init__(self, accessor_manager: JsonAccessorManager, phase: str):
        self.accessor_manager = accessor_manager
        self.pid = os.getpid()
        self.ppid = os.getppid()
        try:
            self.user = os.environ.get('USER')
        except OSError:
            self.user = os.getlogin()
        self.phase = phase
        self.id_date = self.accessor_manager.get("shared_config").get("runtime_config")["execution_date"]
        self.start_date = None
        self.end_date = None
        self.status = "Pending"
        self.code = None
        self.message = "En attente de demarrage"
        self.steps = []

    def __str__(self):
        """Representation textuelle du processus."""
        return (f"DataProcess Information:\n"
                f"  PID        : {self.pid}\n"
                f"  PPID       : {self.ppid}\n"
                f"  User       : {self.user}\n"
                f"  Phase      : {self.phase}\n"
                f"  ID Date    : {self.id_date}\n"
                f"  Start Date : {self.start_date}\n"
                f"  End Date   : {self.end_date}\n"
                f"  Status     : {self.status}\n"
                f"  Code       : {self.code}\n"
                f"  Message    : {self.message}")

    def to_dict(self):
        """Retourne les attributs du processus sous forme de dictionnaire."""
        return {
            "pid": self.pid,
            "ppid": self.ppid,
            "user": self.user,
            "phase": self.phase,
            "id_date": self.id_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "status": self.status,
            "code": self.code,
            "message": self.message
        }

    def start(self, code, message="Processing started"):
        """Marque le traitement comme demarre."""
        self.status = "Started"
        self.code = code
        self.message = message
        self.start_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def process(self, code, message="Processing ongoing"):
        """Marque le traitement comme en cours."""
        self.status = "Ongoing"
        self.code = code
        self.message = message

    def fail(self, code, message="Process failed"):
        """Marque le traitement comme echoue."""
        self.status = "Failed"
        self.code = code
        self.message = message
        self.end_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def complete(self, code, message="Process completed successfully"):
        """Marque le traitement comme termine avec succes."""
        self.status = "Completed"
        self.code = code
        self.message = message
        self.end_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def add_step(self, message):
        """Ajoute une etape au processus."""
        step = DataProcessStep(id=len(self.steps) + 1, process_id=self.pid, message=message)
        self.steps.append(step)

    def get_last_step(self) -> DataProcessStep:
        """Retourne la derniere etape ajoutee, ou None si aucune etape."""
        return self.steps[-1] if self.steps else None

    def reset_steps(self):
        """Reinitialise la liste des etapes du processus."""
        self.steps = []
