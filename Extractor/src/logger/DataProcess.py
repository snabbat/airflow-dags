import os
from datetime import datetime

from context_manager.JsonAccessorManager import JsonAccessorManager
from logger.DataProcessStep import DataProcessStep

class DataProcess:
    def __init__(self, accessor_manager: JsonAccessorManager, phase: str):
        # Récupérer les informations système
        self.accessor_manager = accessor_manager
        self.pid = os.getpid()  # Récupère l'ID du processus actuel
        self.ppid = os.getppid()  # Récupère l'ID du processus parent
        try:
           self.user = os.environ.get('USER') 
        except OSError:
            self.user = os.getlogin()  # Récupère le nom de l'utilisateur courant
        self.phase = phase
        self.id_date = self.accessor_manager.get("shared_config").get("runtime_config")["execution_date"]
        self.start_date = None
        self.end_date = None
        self.status = "Pending"  # Initialisé avec "In Progress"
        self.code = None
        self.message = "En attente de démarrage"
        self.steps = []

    def __str__(self):
        # Générer une représentation de l'objet sous forme de chaîne de caractères
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
        # Retourne un dictionnaire des attributs de l'objet
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
        """Méthode pour marquer le traitement comme démarré"""
        self.status = "Started"
        self.code = code
        self.message = message
        self.start_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def process(self, code, message="Processing ongoing"):
        """Méthode pour marquer le traitement comme en cours"""
        self.status = "Ongoing"
        self.code = code
        self.message = message

    def fail(self, code, message="Process failed"):
        """Méthode pour marquer le traitement comme échoué"""
        self.status = "Failed"
        self.code = code
        self.message = message
        self.end_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def complete(self, code, message="Process completed successfully"):
        """Méthode pour marquer le traitement comme terminé"""
        self.status = "Completed"
        self.code = code
        self.message = message
        self.end_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

    def add_step(self, message):
        step = DataProcessStep(id=len(self.steps) + 1, process_id=self.pid, message=message)
        self.steps.append(step)

    def get_last_step(self) -> DataProcessStep :
        """Retourne la dernière étape ajoutée, ou None s'il n'y a pas d'étapes"""
        return self.steps[-1] if self.steps else None

    def reset_steps(self):
        """Réinitialise la liste des étapes du processus"""
        self.steps = []