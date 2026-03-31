import json
from typing import List, Dict
from datetime import datetime


class LogDataFrameWriter:
    """Collecte les logs en memoire sous forme de liste de dictionnaires pour exploitation ulterieure."""

    def __init__(self):
        self.logs: List[Dict] = []

    def _timestamp(self):
        """Retourne l'horodatage courant au format standard."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _normalize_message(self, message):
        """Normalise le message en chaine de caracteres (serialise les dictionnaires en JSON)."""
        if isinstance(message, dict):
            return json.dumps(message, ensure_ascii=False)
        return str(message)

    def log_data_process(self, data_process: Dict):
        """Enregistre les informations du processus principal dans la liste de logs."""
        enriched = {
            "log_type": "process",
            "phase": data_process.get("phase"),
            "code": data_process.get("code"),
            "message": self._normalize_message(data_process.get("message")),
            "status": data_process.get("status"),
            "timestamp": self._timestamp(),
            "pid": data_process.get("pid"),
            "ppid": data_process.get("ppid"),
            "user": data_process.get("user"),
            "start_date": data_process.get("start_date"),
            "end_date": data_process.get("end_date"),
            "id_date": data_process.get("id_date"),
            "step_id": None,
            "step_datetime": None
        }
        self.logs.append(enriched)

    def log_data_process_step(self, data_process_step: Dict):
        """Enregistre les informations d'une etape specifique dans la liste de logs."""
        enriched = {
            "log_type": "step",
            "phase": data_process_step.get("phase"),
            "code": None,
            "message": self._normalize_message(data_process_step.get("message")),
            "status": "step_recorded",
            "timestamp": self._timestamp(),
            "pid": data_process_step.get("id_process"),
            "ppid": None,
            "user": None,
            "start_date": None,
            "end_date": None,
            "id_date": None,
            "step_id": data_process_step.get("id"),
            "step_datetime": data_process_step.get("datetime")
        }
        self.logs.append(enriched)

    def get_log_dataframe(self):
        """Retourne la liste des logs collectes."""
        return self.logs

    def reset_steps(self):
        """Reinitialise la liste des logs."""
        self.logs = []
