from abc import ABC, abstractmethod


class ExceptionWriter(ABC):
    """Classe abstraite pour les writers d'exceptions. Definit l'interface pour l'ecriture et le formatage."""

    @abstractmethod
    def format_exception(self, error_info: dict) -> str:
        """Formate les informations de l'exception en chaine lisible."""
        pass

    @abstractmethod
    def log_exception(self, formatted_exception: str):
        """Ecrit l'exception formatee vers la destination (fichier, console, etc.)."""
        pass
