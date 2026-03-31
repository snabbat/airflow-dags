# Classe représentant une étape du traitement
from datetime import datetime

class DataProcessStep:
    def __init__(self, id: int, process_id: str, message: str = ""):
        self.id = id
        self.process_id = process_id
        self.datetime = datetime.now()
        self.message = message

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "id_process": self.process_id,
            "datetime": self.datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "message": self.message,
        }