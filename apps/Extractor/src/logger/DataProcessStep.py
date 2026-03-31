from datetime import datetime


class DataProcessStep:
    """Represente une etape individuelle d'un processus de traitement."""

    def __init__(self, id: int, process_id: str, message: str = ""):
        self.id = id
        self.process_id = process_id
        self.datetime = datetime.now()
        self.message = message

    def to_dict(self) -> dict:
        """Retourne les attributs de l'etape sous forme de dictionnaire."""
        return {
            "id": self.id,
            "id_process": self.process_id,
            "datetime": self.datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "message": self.message,
        }
