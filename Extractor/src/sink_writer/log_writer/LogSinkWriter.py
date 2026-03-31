

from abc import ABC, abstractmethod
from typing import Dict

class LogSinkWriter(ABC):
    """
    Abstraction pour un writer de logs.
    """
    @abstractmethod
    def log_data_process(self, data_process: Dict):
        """
        Log les informations du processus principal.
        """
        pass

    @abstractmethod
    def log_data_process_step(self, data_process_step: Dict):
        """
        Log les informations d'une étape spécifique.
        """
        pass