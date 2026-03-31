from context_manager.JsonAccessorManager import JsonAccessorManager
from dag_manager.CoreStrategy import CorePhase
from dag_manager.ExtractionPhase import ExtractionPhase
from dag_manager.MartStrategy import MartsPhase
from dag_manager.PhaseStrategy import PhaseStrategy
from exception_manager.ExceptionManager import ExceptionManager


class PhaseFactory:
    @staticmethod
    def get_phase(accessor_manager: JsonAccessorManager, exception_manager: ExceptionManager, name, recovery_mode="NONE"):
        """Fabrique de phases — retourne la strategie correspondante au nom de la phase."""
        if name == "extraction":
            return ExtractionPhase(accessor_manager, exception_manager, name, recovery_mode)
        elif name == "core":
            return CorePhase(accessor_manager, exception_manager, name, recovery_mode)
        elif name == "marts":
            return MartsPhase(accessor_manager, exception_manager, name, recovery_mode)
        else:
            raise ValueError(f"Phase inconnue : {name}")
