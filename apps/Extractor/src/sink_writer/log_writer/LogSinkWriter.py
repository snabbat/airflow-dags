from abc import ABC, abstractmethod
from typing import Dict


class LogSinkWriter(ABC):
    """Classe abstraite pour les writers de logs. Definit l'interface d'ecriture des processus et etapes."""

    @abstractmethod
    def log_data_process(self, data_process: Dict):
        """Enregistre les informations du processus principal."""
        pass

    @abstractmethod
    def log_data_process_step(self, data_process_step: Dict):
        """Enregistre les informations d'une etape specifique."""
        pass
