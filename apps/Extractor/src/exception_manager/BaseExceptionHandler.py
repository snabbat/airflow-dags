from abc import ABC, abstractmethod


class BaseExceptionHandler(ABC):
    """Classe abstraite pour les gestionnaires d'exceptions dans la chaine de responsabilite."""

    def __init__(self, next_handler=None):
        self.next_handler = next_handler

    def set_next(self, handler):
        """Definit le prochain gestionnaire dans la chaine."""
        self.next_handler = handler
        return handler

    @abstractmethod
    def handle_exception(self, exception, context):
        """Traite l'exception avec le contexte fourni."""
        pass
