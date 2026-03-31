import json
from abc import ABC, abstractmethod


class BaseExceptionHandler(ABC):
    """
    Classe abstraite pour les gestionnaires d'exceptions.
    """
    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def set_next(self, handler):
        self.next_handler = handler
        return handler

    @abstractmethod
    def handle_exception(self, exception, context):
        """
        Méthode pour traiter l'exception.
        """
        pass
